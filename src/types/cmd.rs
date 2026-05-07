use log::trace;

use crate::{
    binary::{protocol, Encoder},
    client_info,
    errors::Result,
    types::{Context, Options, Query, SettingType, Simple},
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
    trace!("[send query] {}", query.get_sql());

    // DBMS_MIN_REVISION_WITH_CLIENT_INFO
    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_QUERY);

    encoder.string(query.get_id());

    {
        let hostname = &context.hostname;
        encoder.uvarint(1);
        encoder.string("");
        encoder.string(query.get_id()); // initial_query_id;
        encoder.string("[::ffff:127.0.0.1]:0");
        if context.server_info.revision
            >= protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME
        {
            // initial_query_start_time_microseconds (int64, fixed 8 bytes)
            encoder.write::<i64>(0i64);
        }
        encoder.uvarint(1); // iface type TCP;
        encoder.string(hostname);
        encoder.string(hostname);
    }
    client_info::write(&mut encoder);

    if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
        encoder.string("");
    }

    if context.server_info.revision
        >= protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH
    {
        // distributed_depth (uvarint, 0 = top-level client query)
        encoder.uvarint(0);
    }

    if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_VERSION_PATCH {
        encoder.uvarint(0);
    }

    if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
        // OpenTelemetry header: 0 = absent, no trace_id/span_id/tracestate/flags follow.
        encoder.write::<u8>(0u8);
    }

    if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
        // collaborate_with_initiator, count_participating_replicas, number_of_current_replica
        encoder.uvarint(0);
        encoder.uvarint(0);
        encoder.uvarint(0);
    }

    let options = context.options.get()?;

    let settings_format = if context.server_info.revision
        >= protocol::DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
    {
        SettingsBinaryFormat::Strings
    } else {
        SettingsBinaryFormat::Old
    };

    serialize_settings(&mut encoder, &options, settings_format);

    if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
        encoder.string(""); // inter-server secret (empty for client→server)
    }

    encoder.uvarint(protocol::STATE_COMPLETE);

    encoder.uvarint(if options.compression {
        protocol::COMPRESS_ENABLE
    } else {
        protocol::COMPRESS_DISABLE
    });

    let options = context.options.get()?;

    encoder.string(query.get_sql());

    // Server-side query parameters (sent AFTER query text, BEFORE the data block).
    // Wire shape per param: <name string><flags varint=2 (Custom)><quoted value | NULL repr>
    // followed by an empty string as the end-of-params marker. Matches
    // clickhouse-cpp Client::Impl::SendQuery exactly.
    if context.server_info.revision >= protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
        const PARAM_FLAG_CUSTOM: u64 = 2;
        let params = query.get_params();
        for (name, value) in params {
            encoder.string(name);
            encoder.uvarint(PARAM_FLAG_CUSTOM);
            match value {
                Some(v) => encoder.quoted_string(v),
                None => encoder.param_null_representation(),
            }
        }
        encoder.string(""); // end-of-params marker
    }

    Block::<Simple>::default().send_data(&mut encoder, options.compression, context.server_info.revision);

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

fn encode_data(block: &Block, context: &Context) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    let options = context.options.get()?;
    block.send_data(&mut encoder, options.compression, context.server_info.revision);
    Ok(encoder.get_buffer())
}

fn encode_union(first: &Cmd, second: &Cmd) -> Result<Vec<u8>> {
    let mut result = encode_command(first)?;
    result.extend((encode_command(second)?).iter());
    Ok(result)
}
