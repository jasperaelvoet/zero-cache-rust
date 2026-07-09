//! Port of `zero-cache/src/types/pg-types.ts`.
//!
//! Built-in Postgres type OIDs (forked from node-pg-types). Only stable
//! `pg_catalog` base type OIDs.

#![allow(missing_docs)]

pub const BOOL: i64 = 16;
pub const BYTEA: i64 = 17;
pub const CHAR: i64 = 18;
pub const INT8: i64 = 20;
pub const INT2: i64 = 21;
pub const INT4: i64 = 23;
pub const REGPROC: i64 = 24;
pub const TEXT: i64 = 25;
pub const OID: i64 = 26;
pub const TID: i64 = 27;
pub const XID: i64 = 28;
pub const CID: i64 = 29;
pub const JSON: i64 = 114;
pub const XML: i64 = 142;
pub const PG_NODE_TREE: i64 = 194;
pub const SMGR: i64 = 210;
pub const PATH: i64 = 602;
pub const POLYGON: i64 = 604;
pub const CIDR: i64 = 650;
pub const FLOAT4: i64 = 700;
pub const FLOAT8: i64 = 701;
pub const ABSTIME: i64 = 702;
pub const RELTIME: i64 = 703;
pub const TINTERVAL: i64 = 704;
pub const CIRCLE: i64 = 718;
pub const MACADDR8: i64 = 774;
pub const MONEY: i64 = 790;
pub const MACADDR: i64 = 829;
pub const INET: i64 = 869;
pub const ACLITEM: i64 = 1033;
pub const BPCHAR: i64 = 1042;
pub const VARCHAR: i64 = 1043;
pub const DATE: i64 = 1082;
pub const TIME: i64 = 1083;
pub const TIMESTAMP: i64 = 1114;
pub const TIMESTAMPTZ: i64 = 1184;
pub const INTERVAL: i64 = 1186;
pub const TIMETZ: i64 = 1266;
pub const BIT: i64 = 1560;
pub const VARBIT: i64 = 1562;
pub const NUMERIC: i64 = 1700;
pub const REFCURSOR: i64 = 1790;
pub const REGPROCEDURE: i64 = 2202;
pub const REGOPER: i64 = 2203;
pub const REGOPERATOR: i64 = 2204;
pub const REGCLASS: i64 = 2205;
pub const REGTYPE: i64 = 2206;
pub const UUID: i64 = 2950;
pub const TXID_SNAPSHOT: i64 = 2970;
pub const PG_LSN: i64 = 3220;
pub const PG_NDISTINCT: i64 = 3361;
pub const PG_DEPENDENCIES: i64 = 3402;
pub const TSVECTOR: i64 = 3614;
pub const TSQUERY: i64 = 3615;
pub const GTSVECTOR: i64 = 3642;
pub const REGCONFIG: i64 = 3734;
pub const REGDICTIONARY: i64 = 3769;
pub const JSONB: i64 = 3802;
pub const REGNAMESPACE: i64 = 4089;
pub const REGROLE: i64 = 4096;

// Array-type OIDs for the base types this port decodes. Postgres's array
// OIDs aren't a fixed offset from their element type, so each needs its own
// constant (values from `pg_type`, matching node-pg-types' well-known
// array OID list). Only base types this crate's `pg_to_change::text_to_json`
// actually type-switches on get an array counterpart.
pub const BOOL_ARRAY: i64 = 1000;
pub const INT2_ARRAY: i64 = 1005;
pub const INT4_ARRAY: i64 = 1007;
pub const TEXT_ARRAY: i64 = 1009;
pub const VARCHAR_ARRAY: i64 = 1015;
pub const INT8_ARRAY: i64 = 1016;
pub const FLOAT4_ARRAY: i64 = 1021;
pub const FLOAT8_ARRAY: i64 = 1022;
pub const NUMERIC_ARRAY: i64 = 1231;
pub const JSON_ARRAY: i64 = 199;
pub const JSONB_ARRAY: i64 = 3807;
pub const UUID_ARRAY: i64 = 2951;

/// Maps a known array-type OID to its element type's OID, or `None` if
/// `oid` isn't one of the array types this crate recognizes.
pub fn array_element_type(oid: i64) -> Option<i64> {
    Some(match oid {
        BOOL_ARRAY => BOOL,
        INT2_ARRAY => INT2,
        INT4_ARRAY => INT4,
        TEXT_ARRAY => TEXT,
        VARCHAR_ARRAY => VARCHAR,
        INT8_ARRAY => INT8,
        FLOAT4_ARRAY => FLOAT4,
        FLOAT8_ARRAY => FLOAT8,
        NUMERIC_ARRAY => NUMERIC,
        JSON_ARRAY => JSON,
        JSONB_ARRAY => JSONB,
        UUID_ARRAY => UUID,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_element_type_maps_known_arrays() {
        assert_eq!(array_element_type(INT4_ARRAY), Some(INT4));
        assert_eq!(array_element_type(TEXT_ARRAY), Some(TEXT));
        assert_eq!(array_element_type(BOOL_ARRAY), Some(BOOL));
    }

    #[test]
    fn array_element_type_none_for_non_array_oid() {
        assert_eq!(array_element_type(INT4), None);
    }
}
