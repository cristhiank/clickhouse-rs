use log::trace;

use crate::{
    binary::{protocol, Encoder},
    client_info,
    errors::{DriverError, Error, Result},
    types::{Context, Options, Query, QueryParameterValue, SettingType, Simple},
    Block,
};

/// Represents Clickhouse commands.
pub(crate) enum Cmd {
    Hello(Context),
    Ping,
    SendQuery(Query, Context),
    SendData(Block, Context),
    Union(Box<Cmd>, Box<Cmd>),
    Cancel,
}

impl Cmd {
    /// Returns the packed command as a byte vector.
    #[inline(always)]
    pub(crate) fn get_packed_command(&self) -> Result<Vec<u8>> {
        encode_command(self)
    }
}

#[derive(Debug, PartialOrd, PartialEq)]
enum SettingsBinaryFormat {
    Old,
    Strings,
}

fn encode_command(cmd: &Cmd) -> Result<Vec<u8>> {
    match cmd {
        Cmd::Hello(context) => encode_hello(context),
        Cmd::Ping => Ok(encode_ping()),
        Cmd::SendQuery(query, context) => encode_query(query, context),
        Cmd::SendData(block, context) => encode_data(block, context),
        Cmd::Union(first, second) => encode_union(first.as_ref(), second.as_ref()),
        Cmd::Cancel => Ok(encode_cancel()),
    }
}

fn encode_hello(context: &Context) -> Result<Vec<u8>> {
    trace!("[hello]        -> {}", client_info::description());

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_HELLO);
    client_info::write(&mut encoder);

    let options = context.options.get()?;

    encoder.string(&options.database);
    encoder.string(&options.username);
    encoder.string(&options.password);

    Ok(encoder.get_buffer())
}

fn encode_ping() -> Vec<u8> {
    trace!("[ping]         -> ping");

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_PING);
    encoder.get_buffer()
}

fn encode_cancel() -> Vec<u8> {
    trace!("[cancel]");

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_CANCEL);
    encoder.get_buffer()
}

fn encode_query(query: &Query, context: &Context) -> Result<Vec<u8>> {
    trace!("[send query] {:?}", query);

    let server_revision = context.server_info.revision;

    // Query parameters require server revision >= DBMS_MIN_REVISION_WITH_PARAMETERS.
    if query.has_parameters() && server_revision < protocol::DBMS_MIN_REVISION_WITH_PARAMETERS {
        return Err(Error::Driver(DriverError::QueryParametersUnsupported {
            server_revision,
            required_revision: protocol::DBMS_MIN_REVISION_WITH_PARAMETERS,
        }));
    }

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_QUERY);

    encoder.string(query.get_id());

    {
        let hostname = &context.hostname;
        encoder.uvarint(1); // initial_type = Initial
        encoder.string(""); // initial_user
        encoder.string(query.get_id()); // initial_query_id
        encoder.string("[::ffff:127.0.0.1]:0"); // initial_address
        if server_revision >= protocol::DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME {
            encoder.write(0_u64); // initial_query_start_time_microseconds
        }
        encoder.uvarint(1); // iface type TCP
        encoder.string(hostname); // os_user
        encoder.string(hostname); // client_hostname
    }
    client_info::write(&mut encoder); // name, major, minor, revision

    if server_revision >= protocol::DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
        encoder.string(""); // quota_key
    }
    if server_revision >= protocol::DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH {
        encoder.uvarint(0); // distributed_depth
    }
    if server_revision >= protocol::DBMS_MIN_REVISION_WITH_VERSION_PATCH {
        encoder.uvarint(0); // version_patch
    }
    if server_revision >= protocol::DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
        encoder.write(0_u8); // OpenTelemetry absent marker
    }
    if server_revision >= protocol::DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
        encoder.uvarint(0); // parallel_replica_offset
        encoder.uvarint(0); // parallel_replicas_count
        encoder.uvarint(0); // parallel_replica_min_number_of_rows
    }

    let options = context.options.get()?;

    let settings_format =
        if server_revision >= protocol::DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
            SettingsBinaryFormat::Strings
        } else {
            SettingsBinaryFormat::Old
        };

    serialize_settings(&mut encoder, &options, settings_format);

    // Interserver secret comes AFTER settings, before stage.
    if server_revision >= protocol::DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
        encoder.string(""); // interserver_secret
    }

    encoder.uvarint(protocol::STATE_COMPLETE);

    encoder.uvarint(if options.compression {
        protocol::COMPRESS_ENABLE
    } else {
        protocol::COMPRESS_DISABLE
    });

    encoder.string(query.get_sql());

    // Write query parameters block; always present when server >= 54459 (even if empty).
    if server_revision >= protocol::DBMS_MIN_REVISION_WITH_PARAMETERS {
        serialize_query_params(&mut encoder, query.get_parameters());
    }

    Block::<Simple>::default().send_data(&mut encoder, options.compression, server_revision);

    Ok(encoder.get_buffer())
}

fn serialize_settings(encoder: &mut Encoder, options: &Options, format: SettingsBinaryFormat) {
    if format < SettingsBinaryFormat::Strings {
        for (name, value) in &options.settings {
            encoder.string(name);
            match &value.value {
                SettingType::String(val) => encoder.string(val),
                // Bool and UInt64 is the same
                SettingType::Bool(val) => encoder.uvarint((val == &true) as u64),
                // Float is written in string representation
                SettingType::Float64(val) => encoder.string(val.to_string()),
                SettingType::UInt64(val) => encoder.uvarint(*val),
            }
        }
    } else {
        for (name, value) in &options.settings {
            encoder.string(name);
            encoder.write(value.is_important);
            encoder.string(value.to_string());
        }
    }

    encoder.string(""); // end of settings marker
}

/// Serialize query parameters using a dedicated custom-flag format (0x02 per param).
/// Wire format: `name | varuint(0x02) | quoted_value_string` for each param, then `""` terminator.
/// This is NOT the same as the settings serializer; do not reuse it.
fn serialize_query_params(encoder: &mut Encoder, params: &[(String, QueryParameterValue)]) {
    for (name, value) in params {
        encoder.string(name);
        encoder.uvarint(0x02); // custom flag
        encoder.string(&value.write_quoted());
    }
    encoder.string(""); // empty name terminator
}

fn encode_data(block: &Block, context: &Context) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    let options = context.options.get()?;
    let server_revision = context.server_info.revision;
    block.send_data(&mut encoder, options.compression, server_revision);
    Ok(encoder.get_buffer())
}

fn encode_union(first: &Cmd, second: &Cmd) -> Result<Vec<u8>> {
    let mut result = encode_command(first)?;
    result.extend((encode_command(second)?).iter());
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        binary::Encoder,
        errors::DriverError,
        types::{query::QueryParameterValue, IntoOptions, Options, OptionsSource, ServerInfo},
    };

    fn make_context(revision: u64) -> Context {
        let mut info = ServerInfo::default();
        info.revision = revision;
        Context {
            options: OptionsSource::default(),
            hostname: "localhost".to_string(),
            server_info: info,
        }
    }

    /// Helper: write query params to a buffer and return bytes.
    fn params_bytes(params: &[(String, QueryParameterValue)]) -> Vec<u8> {
        let mut enc = Encoder::new();
        super::serialize_query_params(&mut enc, params);
        enc.get_buffer()
    }

    #[test]
    fn test_serialize_query_params_empty_terminator() {
        // Empty params → just the terminator (empty string = varint(0) = 0x00).
        let bytes = params_bytes(&[]);
        assert_eq!(bytes, vec![0x00]);
    }

    #[test]
    fn test_serialize_query_params_custom_flag() {
        // One param "a" = "'hello'" should contain the flag byte 0x02.
        let params = vec![("a".to_owned(), QueryParameterValue::from("hello"))];
        let bytes = params_bytes(&params);
        // Name "a" → varint(1) + b'a' = [0x01, b'a']
        // Flag 0x02 → varint(2) = [0x02]
        // Value "'hello'" → varint(7) + b"'hello'" = [0x07, ...]
        // Terminator "" → varint(0) = [0x00]
        assert!(bytes.contains(&0x02), "custom flag 0x02 must be present");
        // The varint for len("'hello'") = 7
        assert!(bytes.contains(&7), "value length varint must be present");
    }

    #[test]
    fn test_serialize_query_params_array_literal_is_quoted_setting_value() {
        // Query parameters are transported as setting strings. For an Array(String)
        // placeholder the raw value must be the inner ClickHouse array literal; this
        // serializer then adds the required outer string quotes for the setting value.
        let params = vec![(
            "array_param".to_owned(),
            QueryParameterValue::from("['Engineering','Branding']"),
        )];
        let bytes = params_bytes(&params);
        let expected_value = b"'[\\'Engineering\\',\\'Branding\\']'";

        assert!(
            bytes
                .windows(expected_value.len())
                .any(|w| w == expected_value),
            "serialized packet must contain the escaped array literal setting value"
        );
    }

    #[test]
    fn test_encode_query_old_server_no_params_succeeds() {
        let ctx = make_context(54400);
        let query = Query::new("SELECT 1");
        let result = encode_query(&query, &ctx);
        assert!(result.is_ok(), "old server + no params must succeed");
    }

    #[test]
    fn test_encode_query_old_server_with_params_fails() {
        let ctx = make_context(54400);
        let query = Query::new("SELECT {v:Int32}")
            .with_parameter("v", 1_i32)
            .unwrap();
        let err = encode_query(&query, &ctx).unwrap_err();
        match err {
            crate::errors::Error::Driver(DriverError::QueryParametersUnsupported {
                server_revision: 54400,
                required_revision: 54459,
            }) => {}
            other => panic!("expected QueryParametersUnsupported, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_query_modern_server_with_params_succeeds() {
        let ctx = make_context(54459);
        let query = Query::new("SELECT {v:Int32}")
            .with_parameter("v", 42_i32)
            .unwrap();
        let result = encode_query(&query, &ctx);
        assert!(result.is_ok(), "modern server + params must succeed");
        let bytes = result.unwrap();
        // The quoted value "'42'" appears somewhere in the packet.
        let needle = b"'42'";
        assert!(
            bytes.windows(needle.len()).any(|w| w == needle),
            "serialized packet must contain \"'42'\""
        );
    }

    #[test]
    fn test_encode_query_modern_server_no_params_has_empty_param_block() {
        let ctx = make_context(54459);
        let query = Query::new("SELECT 1");
        let bytes = encode_query(&query, &ctx).unwrap();
        // At revision 54459, an empty params block (just terminator 0x00) must be written
        // after the SQL string. The exact position is hard to pinpoint, but the packet must
        // be longer than with revision 0 (which would not include the extra fields).
        assert!(!bytes.is_empty());
    }

    /// Verify that the settings block is serialized BEFORE the interserver-secret marker and
    /// not after. Revision 54441 enables both interserver-secret (54441) and Strings settings
    /// format (54429). With a single custom setting, the wire layout after the setting name is:
    ///   [is_important][value_varint][value_bytes][settings_terminator=0x00]
    ///   [interserver_secret=0x00][STATE_COMPLETE=0x02]
    /// If the secret were written before settings (old buggy order) the byte at offset +19
    /// after the setting name would be STATE_COMPLETE (0x02), not 0x00.
    #[test]
    fn test_encode_query_settings_before_interserver_secret() {
        let options = Options::default().with_setting("zz_order_test", "xyz", false);
        let mut info = ServerInfo::default();
        info.revision = 54441;
        let ctx = Context {
            options: options.into_options_src(),
            hostname: "localhost".to_string(),
            server_info: info,
        };
        let query = Query::new("SELECT 1");
        let bytes = encode_query(&query, &ctx).expect("encode must succeed at revision 54441");

        // Locate the setting name bytes in the packet.
        let name = b"zz_order_test"; // len = 13
        let pos = bytes
            .windows(name.len())
            .position(|w| w == name)
            .expect("setting name 'zz_order_test' must appear in encoded packet");

        // After the 13-byte name, in Strings format:
        //   +0  is_important flag  : 0x00  (false)
        //   +1  value len varint   : 0x03  ("xyz" = 3 bytes)
        //   +2..+4  value bytes    : b"xyz"
        //   +5  settings terminator: 0x00
        // Then the correct post-settings bytes:
        //   +6  interserver-secret : 0x00  (empty string varint)
        //   +7  STATE_COMPLETE     : 0x02
        assert_eq!(bytes[pos + 13], 0x00, "is_important flag");
        assert_eq!(bytes[pos + 14], 0x03, "value length varint for 'xyz'");
        assert_eq!(&bytes[pos + 15..pos + 18], b"xyz", "value bytes");
        assert_eq!(bytes[pos + 18], 0x00, "settings terminator");
        assert_eq!(
            bytes[pos + 19],
            0x00,
            "interserver-secret (0x00) must follow settings; \
             got 0x{:02x} — if this is 0x02 the secret was placed before settings (bug)",
            bytes[pos + 19]
        );
        assert_eq!(
            bytes[pos + 20],
            0x02,
            "STATE_COMPLETE must follow interserver-secret"
        );
    }
}
