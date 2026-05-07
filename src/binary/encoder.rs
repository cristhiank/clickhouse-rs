use crate::{
    binary,
    types::{Marshal, StatBuffer},
};

const MAX_VARINT_LEN64: usize = 10;

#[derive(Default)]
pub struct Encoder {
    buffer: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Encoder { buffer: Vec::new() }
    }

    pub fn uvarint(&mut self, v: u64) {
        let mut scratch = [0u8; MAX_VARINT_LEN64];
        let ln = binary::put_uvarint(&mut scratch[..], v);
        self.write_bytes(&scratch[..ln]);
    }

    pub fn string(&mut self, text: impl AsRef<str>) {
        let bytes = text.as_ref().as_bytes();
        self.byte_string(bytes);
    }

    pub fn byte_string(&mut self, source: impl AsRef<[u8]>) {
        self.uvarint(source.as_ref().len() as u64);
        self.write_bytes(source.as_ref());
    }

    pub fn write<T>(&mut self, value: T)
    where
        T: Copy + Marshal + StatBuffer,
    {
        let mut buffer = T::buffer();
        value.marshal(buffer.as_mut());
        self.write_bytes(buffer.as_ref());
    }

    pub fn write_bytes(&mut self, b: &[u8]) {
        self.buffer.extend_from_slice(b);
    }

    /// Encodes a string as a quoted SQL parameter literal, byte-for-byte
    /// equivalent to clickhouse-cpp's `WireFormat::WriteQuotedString`.
    /// Output: `\<varint length\>'<escaped value>'`.
    pub fn quoted_string(&mut self, value: impl AsRef<str>) {
        let bytes = value.as_ref().as_bytes();
        let needs_escape =
            |b: u8| matches!(b, b'\0' | 0x08 | b'\t' | b'\n' | b'\'' | b'\\');
        let quoted_count = bytes.iter().filter(|&&b| needs_escape(b)).count();

        // Each escaped byte contributes 4 wire bytes (1 leading backslash + 3
        // payload bytes), so the total length is original_len + 2 quotes
        // + 3 extra bytes per escaped byte.
        let total_len = bytes.len() + 2 + 3 * quoted_count;
        self.uvarint(total_len as u64);
        self.write_bytes(b"'");

        for &b in bytes {
            match b {
                b'\0' => self.write_bytes(b"\\x00"),
                0x08 => self.write_bytes(b"\\x08"),
                b'\t' => self.write_bytes(b"\\\\\\t"),
                b'\n' => self.write_bytes(b"\\\\\\n"),
                b'\'' => self.write_bytes(b"\\x27"),
                b'\\' => self.write_bytes(b"\\\\\\\\"),
                _ => self.write_bytes(&[b]),
            }
        }

        self.write_bytes(b"'");
    }

    /// Encodes the special NULL parameter representation used by ClickHouse,
    /// matching `WireFormat::WriteParamNullRepresentation` (`'\\N'`).
    pub fn param_null_representation(&mut self) {
        // 5 wire bytes: 0x27 0x5C 0x5C 0x4E 0x27 (i.e. ' \ \ N ')
        self.uvarint(5);
        self.write_bytes(b"'\\\\N'");
    }

    pub fn get_buffer(self) -> Vec<u8> {
        self.buffer
    }

    pub fn get_buffer_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}
