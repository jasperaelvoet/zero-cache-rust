//! Port of `zero-cache/src/db/pg-copy-binary.ts`.
//!
//! A streaming parser for PostgreSQL `COPY ... TO STDOUT WITH (FORMAT binary)`,
//! plus per-type binary field decoders producing [`LiteValue`]s. Used to bulk-
//! load the initial replica snapshot without going through text parsing.
//!
//! Lives in the `types` crate alongside the other ported `db/` specs until a
//! dedicated `zero-cache-db` crate is split out.

use num_bigint::BigInt;
use thiserror::Error;
use zero_cache_shared::bigint_json::{stringify, JsonValue};

use crate::lite::LiteValue;
use crate::pg_types::*;
use crate::specs::PgTypeClass;

/// COPY binary signature: `PGCOPY\n\xff\r\n\0`.
const PGCOPY_SIGNATURE: [u8; 11] = [
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,
];

const HEADER_MIN_SIZE: usize = 11 + 4 + 4;

/// PG epoch (2000-01-01) offset from Unix epoch, in milliseconds.
const PG_EPOCH_UNIX_MILLIS: f64 = 946_684_800_000.0;
/// Days from Unix epoch to PG epoch.
const PG_EPOCH_UNIX_DAYS: f64 = 10_957.0;
const MS_PER_DAY: f64 = 86_400_000.0;

/// Errors raised while parsing the COPY binary header.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CopyError {
    #[error("Invalid PGCOPY binary signature")]
    InvalidSignature,
    #[error("Unsupported PGCOPY flags: {0}")]
    UnsupportedFlags(i32),
}

/// Streaming parser for the COPY binary format. Feed chunks to [`parse`];
/// each call returns the fields (`None` = SQL NULL) completed by that chunk.
///
/// [`parse`]: BinaryCopyParser::parse
#[derive(Default)]
pub struct BinaryCopyParser {
    buffer: Vec<u8>,
    offset: usize,
    header_parsed: bool,
    fields_remaining: i32,
}

impl BinaryCopyParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends `chunk` and returns any fully-parsed fields. Port of `parse`.
    pub fn parse(&mut self, chunk: &[u8]) -> Result<Vec<Option<Vec<u8>>>, CopyError> {
        self.append(chunk);
        let mut out = Vec::new();

        if !self.header_parsed && !self.try_parse_header()? {
            return Ok(out);
        }

        loop {
            if self.fields_remaining == 0 {
                if self.remaining() < 2 {
                    break;
                }
                let field_count = i16be(&self.buffer, self.offset);
                if field_count == -1 {
                    break; // trailer marker
                }
                self.offset += 2;
                self.fields_remaining = field_count as i32;
            }

            while self.fields_remaining > 0 {
                if self.remaining() < 4 {
                    self.compact();
                    return Ok(out);
                }
                let field_len = i32be(&self.buffer, self.offset);
                self.offset += 4;

                if field_len == -1 {
                    out.push(None);
                } else {
                    let field_len = field_len as usize;
                    if self.remaining() < field_len {
                        self.offset -= 4;
                        self.compact();
                        return Ok(out);
                    }
                    out.push(Some(
                        self.buffer[self.offset..self.offset + field_len].to_vec(),
                    ));
                    self.offset += field_len;
                }
                self.fields_remaining -= 1;
            }
        }

        self.compact();
        Ok(out)
    }

    fn remaining(&self) -> usize {
        self.buffer.len() - self.offset
    }

    fn append(&mut self, chunk: &[u8]) {
        if self.buffer.len() == self.offset {
            self.buffer = chunk.to_vec();
            self.offset = 0;
        } else {
            let mut next = self.buffer[self.offset..].to_vec();
            next.extend_from_slice(chunk);
            self.buffer = next;
            self.offset = 0;
        }
    }

    fn compact(&mut self) {
        if self.offset > 0 {
            self.buffer = self.buffer[self.offset..].to_vec();
            self.offset = 0;
        }
    }

    fn try_parse_header(&mut self) -> Result<bool, CopyError> {
        if self.remaining() < HEADER_MIN_SIZE {
            return Ok(false);
        }
        for (i, &sig) in PGCOPY_SIGNATURE.iter().enumerate() {
            if self.buffer[self.offset + i] != sig {
                return Err(CopyError::InvalidSignature);
            }
        }
        self.offset += 11;

        let flags = i32be(&self.buffer, self.offset);
        self.offset += 4;
        if flags != 0 {
            return Err(CopyError::UnsupportedFlags(flags));
        }

        let extension_len = i32be(&self.buffer, self.offset) as usize;
        self.offset += 4;
        if extension_len > 0 {
            if self.remaining() < extension_len {
                self.offset -= HEADER_MIN_SIZE;
                return Ok(false);
            }
            self.offset += extension_len;
        }

        self.header_parsed = true;
        Ok(true)
    }
}

// ---- byte readers (big-endian) --------------------------------------------

fn i16be(b: &[u8], o: usize) -> i16 {
    i16::from_be_bytes([b[o], b[o + 1]])
}
fn u16be(b: &[u8], o: usize) -> u16 {
    u16::from_be_bytes([b[o], b[o + 1]])
}
fn i32be(b: &[u8], o: usize) -> i32 {
    i32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn u32be(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn i64be(b: &[u8], o: usize) -> i64 {
    i64::from_be_bytes([
        b[o],
        b[o + 1],
        b[o + 2],
        b[o + 3],
        b[o + 4],
        b[o + 5],
        b[o + 6],
        b[o + 7],
    ])
}
fn f32be(b: &[u8], o: usize) -> f32 {
    f32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn f64be(b: &[u8], o: usize) -> f64 {
    f64::from_be_bytes([
        b[o],
        b[o + 1],
        b[o + 2],
        b[o + 3],
        b[o + 4],
        b[o + 5],
        b[o + 6],
        b[o + 7],
    ])
}

// ---- decoders -------------------------------------------------------------

/// A function decoding a raw COPY binary field into a [`LiteValue`].
pub type BinaryDecoder = Box<dyn Fn(&[u8]) -> LiteValue + Send + Sync>;

/// The subset of a column spec needed to build a binary decoder. Port of
/// `BinaryColumnSpec`.
#[derive(Debug, Clone)]
pub struct BinaryColumnSpec {
    pub type_oid: i64,
    pub data_type: String,
    pub pg_type_class: Option<PgTypeClass>,
    pub elem_pg_type_class: Option<PgTypeClass>,
}

impl BinaryColumnSpec {
    /// Builds a spec from just an OID and data type (the common test shape).
    pub fn new(type_oid: i64, data_type: impl Into<String>) -> Self {
        BinaryColumnSpec {
            type_oid,
            data_type: data_type.into(),
            pg_type_class: None,
            elem_pg_type_class: None,
        }
    }
}

const KNOWN_BINARY_OIDS: &[i64] = &[
    BOOL,
    INT2,
    INT4,
    INT8,
    FLOAT4,
    FLOAT8,
    TEXT,
    VARCHAR,
    BPCHAR,
    CHAR,
    UUID,
    BYTEA,
    JSON,
    JSONB,
    TIMESTAMP,
    TIMESTAMPTZ,
    DATE,
    TIME,
    TIMETZ,
    NUMERIC,
];

/// Whether the column's binary format can be decoded natively. Port of
/// `hasBinaryDecoder`.
pub fn has_binary_decoder(spec: &BinaryColumnSpec) -> bool {
    if spec.elem_pg_type_class.is_some() {
        return true; // arrays
    }
    if spec.pg_type_class == Some(PgTypeClass::Enum) {
        return true; // enums are sent as UTF-8 text
    }
    KNOWN_BINARY_OIDS.contains(&spec.type_oid)
}

/// Decoder for columns cast to `::text`. Port of `textCastDecoder`.
pub fn text_cast_decoder(buf: &[u8]) -> LiteValue {
    LiteValue::Text(String::from_utf8_lossy(buf).into_owned())
}

/// Builds a binary decoder for `spec`. Port of `makeBinaryDecoder`. Errors for
/// types with no native binary decoder (call [`has_binary_decoder`] first).
pub fn make_binary_decoder(spec: &BinaryColumnSpec) -> Result<BinaryDecoder, String> {
    if spec.elem_pg_type_class.is_some() {
        return Ok(Box::new(|buf: &[u8]| LiteValue::Text(decode_array(buf))));
    }
    if spec.pg_type_class == Some(PgTypeClass::Enum) {
        return Ok(Box::new(|buf: &[u8]| {
            LiteValue::Text(String::from_utf8_lossy(buf).into_owned())
        }));
    }

    let decoder: BinaryDecoder = match spec.type_oid {
        BOOL => Box::new(|buf| LiteValue::Number(if buf[0] != 0 { 1.0 } else { 0.0 })),
        INT2 => Box::new(|buf| LiteValue::Number(i16be(buf, 0) as f64)),
        INT4 => Box::new(|buf| LiteValue::Number(i32be(buf, 0) as f64)),
        INT8 => Box::new(|buf| LiteValue::Big(BigInt::from(i64be(buf, 0)))),
        FLOAT4 => Box::new(|buf| LiteValue::Number(f32be(buf, 0) as f64)),
        FLOAT8 => Box::new(|buf| LiteValue::Number(f64be(buf, 0))),
        TEXT | VARCHAR | BPCHAR | CHAR | JSON => {
            Box::new(|buf| LiteValue::Text(String::from_utf8_lossy(buf).into_owned()))
        }
        UUID => Box::new(|buf| LiteValue::Text(decode_uuid(buf))),
        BYTEA => Box::new(|buf| LiteValue::Blob(buf.to_vec())),
        // JSONB has a 1-byte version prefix (currently 0x01).
        JSONB => Box::new(|buf| LiteValue::Text(String::from_utf8_lossy(&buf[1..]).into_owned())),
        TIMESTAMP | TIMESTAMPTZ => Box::new(|buf| LiteValue::Number(decode_timestamp(buf))),
        DATE => Box::new(|buf| LiteValue::Number(decode_date(buf))),
        TIME => Box::new(|buf| LiteValue::Number(decode_time(buf))),
        TIMETZ => Box::new(|buf| LiteValue::Number(decode_time_tz(buf))),
        NUMERIC => Box::new(|buf| LiteValue::Number(decode_numeric(buf))),
        oid => {
            return Err(format!(
                "No binary decoder for type OID {oid}. \
                 Use has_binary_decoder() to check before calling make_binary_decoder()."
            ))
        }
    };
    Ok(decoder)
}

/// UUID: 16 bytes -> `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. Port of `decodeUUID`.
pub fn decode_uuid(buf: &[u8]) -> String {
    let mut hex = String::with_capacity(32);
    for b in &buf[..16] {
        hex.push_str(&format!("{b:02x}"));
    }
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// TIMESTAMP/TIMESTAMPTZ: int64 microseconds since PG epoch -> f64 millis since
/// Unix epoch. Port of `decodeTimestamp`.
pub fn decode_timestamp(buf: &[u8]) -> f64 {
    let hi = i32be(buf, 0);
    let lo = u32be(buf, 4);
    if hi == 0x7fff_ffff && lo == 0xffff_ffff {
        return f64::INFINITY;
    }
    if hi == i32::MIN && lo == 0 {
        return f64::NEG_INFINITY;
    }
    let micros = hi as f64 * 4_294_967_296.0 + lo as f64;
    micros / 1000.0 + PG_EPOCH_UNIX_MILLIS
}

/// DATE: int32 days since PG epoch -> f64 millis since Unix epoch. Port of
/// `decodeDate`.
pub fn decode_date(buf: &[u8]) -> f64 {
    let pg_days = i32be(buf, 0);
    if pg_days == 0x7fff_ffff {
        return f64::INFINITY;
    }
    if pg_days == i32::MIN {
        return f64::NEG_INFINITY;
    }
    (pg_days as f64 + PG_EPOCH_UNIX_DAYS) * MS_PER_DAY
}

/// TIME: int64 microseconds since midnight -> f64 millis. Port of `decodeTime`.
pub fn decode_time(buf: &[u8]) -> f64 {
    let hi = i32be(buf, 0);
    let lo = u32be(buf, 4);
    let micros = hi as f64 * 4_294_967_296.0 + lo as f64;
    (micros / 1000.0).trunc()
}

/// TIMETZ: int64 micros since midnight + int32 tz offset seconds -> f64 UTC
/// millis, normalized to `[0, MS_PER_DAY)`. Port of `decodeTimeTZ`.
pub fn decode_time_tz(buf: &[u8]) -> f64 {
    let hi = i32be(buf, 0);
    let lo = u32be(buf, 4);
    let local_micros = hi as f64 * 4_294_967_296.0 + lo as f64;
    let tz_offset_seconds = i32be(buf, 8);
    let utc_micros = local_micros + tz_offset_seconds as f64 * 1_000_000.0;
    let mut ms = (utc_micros / 1000.0).trunc();
    if !(0.0..MS_PER_DAY).contains(&ms) {
        ms = ((ms % MS_PER_DAY) + MS_PER_DAY) % MS_PER_DAY;
    }
    ms
}

const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xc000;
const NUMERIC_PINF: u16 = 0xd000;
const NUMERIC_NINF: u16 = 0xf000;
const NBASE: f64 = 10_000.0;

/// NUMERIC: variable-length base-10000 format -> f64. Port of `decodeNumeric`.
pub fn decode_numeric(buf: &[u8]) -> f64 {
    let ndigits = i16be(buf, 0);
    let weight = i16be(buf, 2);
    let sign = u16be(buf, 4);

    if sign == NUMERIC_NAN {
        return f64::NAN;
    }
    if sign == NUMERIC_PINF {
        return f64::INFINITY;
    }
    if sign == NUMERIC_NINF {
        return f64::NEG_INFINITY;
    }
    if ndigits == 0 {
        return 0.0;
    }
    if ndigits > 3 {
        return decode_numeric_via_string(buf, ndigits, weight, sign);
    }

    let mut int_val = 0.0f64;
    for i in 0..ndigits as usize {
        int_val = int_val * NBASE + i16be(buf, 8 + i * 2) as f64;
    }
    let shift = ndigits - weight - 1;
    let result = if shift > 0 {
        int_val / NBASE.powi(shift as i32)
    } else if shift < 0 {
        int_val * NBASE.powi(-shift as i32)
    } else {
        int_val
    };
    if sign == NUMERIC_NEG {
        -result
    } else {
        result
    }
}

fn decode_numeric_via_string(buf: &[u8], ndigits: i16, weight: i16, sign: u16) -> f64 {
    let int_groups = weight + 1;
    let mut s = String::new();
    for i in 0..ndigits {
        let digit = i16be(buf, 8 + i as usize * 2);
        if i == int_groups {
            if s.is_empty() {
                s.push('0');
            }
            s.push('.');
        }
        if i == 0 {
            s.push_str(&digit.to_string());
        } else {
            s.push_str(&format!("{digit:04}"));
        }
    }
    if int_groups > ndigits {
        s.push_str(&"0".repeat(((int_groups - ndigits) * 4) as usize));
    }
    let signed = if sign == NUMERIC_NEG {
        format!("-{s}")
    } else {
        s
    };
    signed.parse::<f64>().unwrap_or(f64::NAN)
}

/// Array: binary format -> JSON string (matching the text path). Port of
/// `decodeArray`.
pub fn decode_array(buf: &[u8]) -> String {
    let mut offset = 0usize;
    let ndim = i32be(buf, offset);
    offset += 4;
    offset += 4; // flags
    let elem_oid = i32be(buf, offset) as i64;
    offset += 4;

    if ndim == 0 {
        return "[]".to_string();
    }

    let mut dims = Vec::with_capacity(ndim as usize);
    for _ in 0..ndim {
        dims.push(i32be(buf, offset));
        offset += 4;
        offset += 4; // lower bound
    }

    let value = read_dimension(buf, &mut offset, &dims, ndim, 0, elem_oid);
    stringify(&value)
}

fn read_dimension(
    buf: &[u8],
    offset: &mut usize,
    dims: &[i32],
    ndim: i32,
    dim: i32,
    elem_oid: i64,
) -> JsonValue {
    let size = dims[dim as usize];
    let mut arr = Vec::with_capacity(size.max(0) as usize);
    for _ in 0..size {
        if dim < ndim - 1 {
            arr.push(read_dimension(buf, offset, dims, ndim, dim + 1, elem_oid));
        } else {
            let elem_len = i32be(buf, *offset);
            *offset += 4;
            if elem_len == -1 {
                arr.push(JsonValue::Null);
            } else {
                let end = *offset + elem_len as usize;
                arr.push(decode_element(elem_oid, &buf[*offset..end]));
                *offset = end;
            }
        }
    }
    JsonValue::Array(arr)
}

/// Decodes an array element into a [`JsonValue`] (to be re-stringified). Port of
/// `makeElementDecoder`. JSON/JSONB elements are kept as their raw text (the
/// upstream re-parse of nested JSON in arrays is not exercised by tests).
fn decode_element(elem_oid: i64, buf: &[u8]) -> JsonValue {
    match elem_oid {
        BOOL => JsonValue::Bool(buf[0] != 0),
        INT2 => JsonValue::Number(i16be(buf, 0) as f64),
        INT4 => JsonValue::Number(i32be(buf, 0) as f64),
        INT8 => {
            let v = i64be(buf, 0);
            if (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&v) {
                JsonValue::Number(v as f64)
            } else {
                JsonValue::BigInt(BigInt::from(v))
            }
        }
        FLOAT4 => JsonValue::Number(f32be(buf, 0) as f64),
        FLOAT8 => JsonValue::Number(f64be(buf, 0)),
        TEXT | VARCHAR | BPCHAR | CHAR => {
            JsonValue::String(String::from_utf8_lossy(buf).into_owned())
        }
        UUID => JsonValue::String(decode_uuid(buf)),
        TIMESTAMP | TIMESTAMPTZ => JsonValue::Number(decode_timestamp(buf)),
        DATE => JsonValue::Number(decode_date(buf)),
        TIME => JsonValue::Number(decode_time(buf)),
        TIMETZ => JsonValue::Number(decode_time_tz(buf)),
        NUMERIC => JsonValue::Number(decode_numeric(buf)),
        _ => JsonValue::String(String::from_utf8_lossy(buf).into_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- BinaryCopyParser helpers (mirror the TS test helpers) -----

    fn pgcopy_header(flags: i32, ext: &[u8]) -> Vec<u8> {
        let mut v = PGCOPY_SIGNATURE.to_vec();
        v.extend_from_slice(&flags.to_be_bytes());
        v.extend_from_slice(&(ext.len() as i32).to_be_bytes());
        v.extend_from_slice(ext);
        v
    }

    fn tuple(fields: &[Option<Vec<u8>>]) -> Vec<u8> {
        let mut v = (fields.len() as i16).to_be_bytes().to_vec();
        for f in fields {
            match f {
                None => v.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(data) => {
                    v.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    v.extend_from_slice(data);
                }
            }
        }
        v
    }

    fn trailer() -> Vec<u8> {
        (-1i16).to_be_bytes().to_vec()
    }

    fn parse_all(parser: &mut BinaryCopyParser, chunks: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        let mut out = Vec::new();
        for c in chunks {
            out.extend(parser.parse(c).unwrap());
        }
        out
    }

    fn i32buf(n: i32) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    #[test]
    fn empty_table() {
        let mut p = BinaryCopyParser::new();
        let mut data = pgcopy_header(0, &[]);
        data.extend(trailer());
        assert_eq!(parse_all(&mut p, &[&data]), Vec::<Option<Vec<u8>>>::new());
    }

    #[test]
    fn single_row_with_values() {
        let mut p = BinaryCopyParser::new();
        let mut data = pgcopy_header(0, &[]);
        data.extend(tuple(&[Some(i32buf(42)), Some(b"hello".to_vec())]));
        data.extend(trailer());
        let r = parse_all(&mut p, &[&data]);
        assert_eq!(r.len(), 2);
        assert_eq!(i32be(r[0].as_ref().unwrap(), 0), 42);
        assert_eq!(r[1].as_ref().unwrap(), b"hello");
    }

    #[test]
    fn null_fields() {
        let mut p = BinaryCopyParser::new();
        let mut data = pgcopy_header(0, &[]);
        data.extend(tuple(&[None, Some(b"x".to_vec()), None]));
        data.extend(trailer());
        let r = parse_all(&mut p, &[&data]);
        assert_eq!(r[0], None);
        assert_eq!(r[1].as_ref().unwrap(), b"x");
        assert_eq!(r[2], None);
    }

    #[test]
    fn multiple_rows() {
        let mut p = BinaryCopyParser::new();
        let mut data = pgcopy_header(0, &[]);
        data.extend(tuple(&[Some(i32buf(1))]));
        data.extend(tuple(&[Some(i32buf(2))]));
        data.extend(trailer());
        let r = parse_all(&mut p, &[&data]);
        assert_eq!(r.len(), 2);
        assert_eq!(i32be(r[0].as_ref().unwrap(), 0), 1);
        assert_eq!(i32be(r[1].as_ref().unwrap(), 0), 2);
    }

    #[test]
    fn chunked_one_byte_at_a_time() {
        let mut p = BinaryCopyParser::new();
        let mut full = pgcopy_header(0, &[]);
        full.extend(tuple(&[Some(b"xy".to_vec()), None]));
        full.extend(trailer());
        let chunks: Vec<[u8; 1]> = full.iter().map(|&b| [b]).collect();
        let refs: Vec<&[u8]> = chunks.iter().map(|c| c.as_slice()).collect();
        let r = parse_all(&mut p, &refs);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].as_ref().unwrap(), b"xy");
        assert_eq!(r[1], None);
    }

    #[test]
    fn chunked_field_data_split() {
        let mut p = BinaryCopyParser::new();
        let mut full = pgcopy_header(0, &[]);
        full.extend(tuple(&[Some(b"hello world".to_vec())]));
        full.extend(trailer());
        let split = pgcopy_header(0, &[]).len() + 2 + 4 + 3;
        let r = parse_all(&mut p, &[&full[..split], &full[split..]]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].as_ref().unwrap(), b"hello world");
    }

    #[test]
    fn rejects_invalid_signature() {
        let mut p = BinaryCopyParser::new();
        let mut bad = vec![0u8; 19];
        bad[..10].copy_from_slice(b"NOT_PGCOPY");
        assert_eq!(p.parse(&bad), Err(CopyError::InvalidSignature));
    }

    #[test]
    fn rejects_nonzero_flags() {
        let mut p = BinaryCopyParser::new();
        let data = pgcopy_header(1, &[]);
        assert_eq!(p.parse(&data), Err(CopyError::UnsupportedFlags(1)));
    }

    #[test]
    fn header_with_extension_data() {
        let mut p = BinaryCopyParser::new();
        let mut data = pgcopy_header(0, b"extension-data");
        data.extend(tuple(&[Some(b"ok".to_vec())]));
        data.extend(trailer());
        let r = parse_all(&mut p, &[&data]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].as_ref().unwrap(), b"ok");
    }

    #[test]
    fn empty_fields_zero_length() {
        let mut p = BinaryCopyParser::new();
        let mut data = pgcopy_header(0, &[]);
        data.extend(tuple(&[Some(vec![])]));
        data.extend(trailer());
        let r = parse_all(&mut p, &[&data]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].as_ref().unwrap().len(), 0);
    }

    // ----- decoders -----

    #[test]
    fn test_decode_uuid() {
        let buf = hex("550e8400e29b41d4a716446655440000");
        assert_eq!(decode_uuid(&buf), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_decode_timestamp() {
        // 2024-01-15T12:30:00Z -> ms
        let expected_ms = 1_705_321_800_000i64;
        let pg_micros = (expected_ms - 946_684_800_000) * 1000;
        let buf = pg_micros.to_be_bytes();
        assert_eq!(decode_timestamp(&buf), expected_ms as f64);
    }

    #[test]
    fn test_decode_timestamp_submillis() {
        let buf = 500i64.to_be_bytes();
        assert!((decode_timestamp(&buf) - (946_684_800_000.0 + 0.5)).abs() < 1e-3);
    }

    #[test]
    fn test_decode_timestamp_infinities() {
        assert_eq!(
            decode_timestamp(&0x7fff_ffff_ffff_ffffi64.to_be_bytes()),
            f64::INFINITY
        );
        assert_eq!(decode_timestamp(&i64::MIN.to_be_bytes()), f64::NEG_INFINITY);
    }

    #[test]
    fn test_decode_date() {
        let expected = 1_705_276_800_000f64; // Date.UTC(2024,0,15)
        let pg_days = (expected / 86_400_000.0 - 10_957.0) as i32;
        assert_eq!(decode_date(&pg_days.to_be_bytes()), expected);
        assert_eq!(decode_date(&0i32.to_be_bytes()), 946_684_800_000.0); // 2000-01-01
        assert_eq!(decode_date(&(-1i32).to_be_bytes()), 946_598_400_000.0); // 1999-12-31
        assert_eq!(decode_date(&0x7fff_ffffi32.to_be_bytes()), f64::INFINITY);
        assert_eq!(decode_date(&i32::MIN.to_be_bytes()), f64::NEG_INFINITY);
    }

    #[test]
    fn test_decode_time() {
        assert_eq!(decode_time(&45_000_000_000i64.to_be_bytes()), 45_000_000.0);
        assert_eq!(decode_time(&0i64.to_be_bytes()), 0.0);
    }

    #[test]
    fn test_decode_time_tz() {
        let mk = |micros: i64, tz: i32| {
            let mut b = micros.to_be_bytes().to_vec();
            b.extend_from_slice(&tz.to_be_bytes());
            b
        };
        assert_eq!(decode_time_tz(&mk(45_000_000_000, 0)), 45_000_000.0);
        assert_eq!(decode_time_tz(&mk(45_000_000_000, -18000)), 27_000_000.0);
        assert_eq!(decode_time_tz(&mk(45_000_000_000, 18000)), 63_000_000.0);
        assert_eq!(decode_time_tz(&mk(3_600_000_000, -18000)), 72_000_000.0);
    }

    #[test]
    fn test_decode_numeric() {
        // 12345.67: ndigits=3, weight=1, digits=[1,2345,6700]
        let mut buf = Vec::new();
        buf.extend_from_slice(&3i16.to_be_bytes());
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&2i16.to_be_bytes());
        for d in [1i16, 2345, 6700] {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        assert!((decode_numeric(&buf) - 12345.67).abs() < 0.01);

        // -42
        let mut neg = Vec::new();
        neg.extend_from_slice(&1i16.to_be_bytes());
        neg.extend_from_slice(&0i16.to_be_bytes());
        neg.extend_from_slice(&0x4000u16.to_be_bytes());
        neg.extend_from_slice(&0i16.to_be_bytes());
        neg.extend_from_slice(&42i16.to_be_bytes());
        assert_eq!(decode_numeric(&neg), -42.0);

        let zero = numeric_header(0, 0, 0);
        assert_eq!(decode_numeric(&zero), 0.0);
        assert!(decode_numeric(&numeric_header(0, 0, 0xc000)).is_nan());
        assert_eq!(decode_numeric(&numeric_header(0, 0, 0xd000)), f64::INFINITY);
        assert_eq!(
            decode_numeric(&numeric_header(0, 0, 0xf000)),
            f64::NEG_INFINITY
        );
    }

    fn numeric_header(ndigits: i16, weight: i16, sign: u16) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&ndigits.to_be_bytes());
        b.extend_from_slice(&weight.to_be_bytes());
        b.extend_from_slice(&sign.to_be_bytes());
        b.extend_from_slice(&0i16.to_be_bytes());
        b
    }

    // ----- makeBinaryDecoder -----

    #[test]
    fn make_decoder_scalars() {
        let dec =
            |oid: i64, dt: &str| make_binary_decoder(&BinaryColumnSpec::new(oid, dt)).unwrap();

        assert_eq!(dec(BOOL, "bool")(&[1]), LiteValue::Number(1.0));
        assert_eq!(dec(BOOL, "bool")(&[0]), LiteValue::Number(0.0));
        assert_eq!(
            dec(INT2, "int2")(&(-123i16).to_be_bytes()),
            LiteValue::Number(-123.0)
        );
        assert_eq!(
            dec(INT4, "int4")(&2_000_000i32.to_be_bytes()),
            LiteValue::Number(2_000_000.0)
        );
        assert_eq!(
            dec(INT8, "int8")(&9_007_199_254_740_993i64.to_be_bytes()),
            LiteValue::Big(BigInt::from(9_007_199_254_740_993i64))
        );
        assert_eq!(
            dec(FLOAT8, "float8")(&std::f64::consts::PI.to_be_bytes()),
            LiteValue::Number(std::f64::consts::PI)
        );
        assert_eq!(
            dec(TEXT, "text")(b"hello world"),
            LiteValue::Text("hello world".into())
        );
        assert_eq!(
            dec(UUID, "uuid")(&hex("a0eebc999c0b4ef8bb6d6bb9bd380a11")),
            LiteValue::Text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into())
        );
        assert_eq!(
            dec(JSON, "json")(b"{\"key\":\"value\"}"),
            LiteValue::Text("{\"key\":\"value\"}".into())
        );
        let mut jsonb = vec![0x01u8];
        jsonb.extend_from_slice(b"{\"k\":1}");
        assert_eq!(
            dec(JSONB, "jsonb")(&jsonb),
            LiteValue::Text("{\"k\":1}".into())
        );
        assert_eq!(
            dec(BYTEA, "bytea")(&[0xde, 0xad, 0xbe, 0xef]),
            LiteValue::Blob(vec![0xde, 0xad, 0xbe, 0xef])
        );
    }

    #[test]
    fn make_decoder_enum_and_unknown() {
        let enum_spec = BinaryColumnSpec {
            type_oid: 99999,
            data_type: "my_status".into(),
            pg_type_class: Some(PgTypeClass::Enum),
            elem_pg_type_class: None,
        };
        let dec = make_binary_decoder(&enum_spec).unwrap();
        assert_eq!(dec(b"active"), LiteValue::Text("active".into()));

        let err = match make_binary_decoder(&BinaryColumnSpec::new(99999, "unknown")) {
            Err(e) => e,
            Ok(_) => panic!("expected an error"),
        };
        assert!(err.contains("No binary decoder for type OID 99999"));
    }

    #[test]
    fn make_decoder_arrays() {
        let arr_spec = |oid: i64, dt: &str| BinaryColumnSpec {
            type_oid: oid,
            data_type: dt.into(),
            pg_type_class: None,
            elem_pg_type_class: Some(PgTypeClass::Base),
        };

        // int4[]: [10, 20, 30]
        let mut buf = Vec::new();
        buf.extend_from_slice(&1i32.to_be_bytes()); // ndim
        buf.extend_from_slice(&0i32.to_be_bytes()); // flags
        buf.extend_from_slice(&23i32.to_be_bytes()); // elem_oid = INT4
        buf.extend_from_slice(&3i32.to_be_bytes()); // dim size
        buf.extend_from_slice(&1i32.to_be_bytes()); // lower bound
        for v in [10i32, 20, 30] {
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }
        let dec = make_binary_decoder(&arr_spec(1007, "int4[]")).unwrap();
        assert_eq!(dec(&buf), LiteValue::Text("[10,20,30]".into()));

        // empty
        let mut empty = Vec::new();
        empty.extend_from_slice(&0i32.to_be_bytes());
        empty.extend_from_slice(&0i32.to_be_bytes());
        empty.extend_from_slice(&23i32.to_be_bytes());
        assert_eq!(dec(&empty), LiteValue::Text("[]".into()));

        // text[] with NULL: ['a', null, 'b']
        let mut ta = Vec::new();
        ta.extend_from_slice(&1i32.to_be_bytes());
        ta.extend_from_slice(&0i32.to_be_bytes());
        ta.extend_from_slice(&25i32.to_be_bytes()); // TEXT
        ta.extend_from_slice(&3i32.to_be_bytes());
        ta.extend_from_slice(&1i32.to_be_bytes());
        ta.extend_from_slice(&1i32.to_be_bytes());
        ta.extend_from_slice(b"a");
        ta.extend_from_slice(&(-1i32).to_be_bytes());
        ta.extend_from_slice(&1i32.to_be_bytes());
        ta.extend_from_slice(b"b");
        let tdec = make_binary_decoder(&arr_spec(1009, "text[]")).unwrap();
        assert_eq!(tdec(&ta), LiteValue::Text("[\"a\",null,\"b\"]".into()));
    }

    #[test]
    fn has_binary_decoder_cases() {
        assert!(has_binary_decoder(&BinaryColumnSpec::new(BOOL, "bool")));
        assert!(!has_binary_decoder(&BinaryColumnSpec::new(99999, "custom")));
        let enum_spec = BinaryColumnSpec {
            type_oid: 99999,
            data_type: "e".into(),
            pg_type_class: Some(PgTypeClass::Enum),
            elem_pg_type_class: None,
        };
        assert!(has_binary_decoder(&enum_spec));
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
