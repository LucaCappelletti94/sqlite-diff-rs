//! Tests for the `PgBinary` wire source: decoding PostgreSQL binary
//! result fields straight into `Value` via the crate's decoder vocabulary.
//!
//! Covers the v1 supported set (bool, int, real, text, bytea, uuid),
//! NULL short-circuit, malformed-input errors, and `TypeMap` dispatch.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use sqlite_diff_rs::{
    BoolDecoder, DecimalTextDecoder, DecodeError, IntDecoder, PgBinary, PgBinaryColumn,
    PgByteaBinaryDecoder, RealDecoder, TextDecoder, TypeMap, UuidBlob16Decoder, Value, WireAdapter,
    WireType,
};

/// The 16 raw bytes of `550e8400-e29b-41d4-a716-446655440000`.
const UUID_BYTES: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
];

fn col(wire_type: WireType, raw: Option<&[u8]>) -> PgBinaryColumn<'_> {
    PgBinaryColumn {
        column_name: "c",
        wire_type,
        raw,
    }
}

// -- Golden per-decoder vectors ----------------------------------------------

#[test]
fn bool_true_and_false() {
    let t: Value<String, Vec<u8>> = col(WireType::Bool, Some(&[0x01]))
        .decoded_by(&BoolDecoder)
        .unwrap();
    let f: Value<String, Vec<u8>> = col(WireType::Bool, Some(&[0x00]))
        .decoded_by(&BoolDecoder)
        .unwrap();
    assert_eq!(t, Value::Integer(1));
    assert_eq!(f, Value::Integer(0));
}

#[test]
fn int_widths() {
    let i2: Value<String, Vec<u8>> = col(WireType::Int, Some(&[0x00, 0x05]))
        .decoded_by(&IntDecoder)
        .unwrap();
    assert_eq!(i2, Value::Integer(5));

    let i4: Value<String, Vec<u8>> = col(WireType::Int, Some(&[0, 0, 0, 42]))
        .decoded_by(&IntDecoder)
        .unwrap();
    assert_eq!(i4, Value::Integer(42));

    let neg = (-7_i64).to_be_bytes();
    let i8: Value<String, Vec<u8>> = col(WireType::Int, Some(&neg))
        .decoded_by(&IntDecoder)
        .unwrap();
    assert_eq!(i8, Value::Integer(-7));
}

#[test]
fn real_widths() {
    let f8bytes = 3.5_f64.to_be_bytes();
    let f8: Value<String, Vec<u8>> = col(WireType::Real, Some(&f8bytes))
        .decoded_by(&RealDecoder)
        .unwrap();
    assert_eq!(f8, Value::Real(3.5));

    let f4bytes = 1.5_f32.to_be_bytes();
    let f4: Value<String, Vec<u8>> = col(WireType::Real, Some(&f4bytes))
        .decoded_by(&RealDecoder)
        .unwrap();
    assert_eq!(f4, Value::Real(1.5));
}

#[test]
fn text_utf8() {
    let s: Value<String, Vec<u8>> = col(WireType::Text, Some(b"hello"))
        .decoded_by(&TextDecoder)
        .unwrap();
    assert_eq!(s, Value::Text(String::from("hello")));
}

#[test]
fn bytea_verbatim() {
    let b: Value<String, Vec<u8>> = col(WireType::Bytes, Some(&[0xDE, 0xAD]))
        .decoded_by(&PgByteaBinaryDecoder)
        .unwrap();
    assert_eq!(b, Value::Blob(alloc::vec![0xDE, 0xAD]));
}

#[test]
fn real_nan_normalizes_to_null() {
    let f8: Value<String, Vec<u8>> = col(WireType::Real, Some(&f64::NAN.to_be_bytes()))
        .decoded_by(&RealDecoder)
        .unwrap();
    assert_eq!(f8, Value::Null);
    let f4: Value<String, Vec<u8>> = col(WireType::Real, Some(&f32::NAN.to_be_bytes()))
        .decoded_by(&RealDecoder)
        .unwrap();
    assert_eq!(f4, Value::Null);
}

#[test]
fn real_negative_zero_normalizes_to_positive_zero() {
    // 0.0 == -0.0 under `==`, so assert the sign bit to prove normalization.
    for bytes in [
        (-0.0_f64).to_be_bytes().to_vec(),
        (-0.0_f32).to_be_bytes().to_vec(),
    ] {
        let got: Value<String, Vec<u8>> = col(WireType::Real, Some(&bytes))
            .decoded_by(&RealDecoder)
            .unwrap();
        match got {
            Value::Real(r) => assert!(r.is_sign_positive(), "expected +0.0, got {r}"),
            other => panic!("expected Value::Real, got {other:?}"),
        }
    }
}

#[test]
fn bytea_empty_slice_yields_empty_blob() {
    let b: Value<String, Vec<u8>> = col(WireType::Bytes, Some(&[]))
        .decoded_by(&PgByteaBinaryDecoder)
        .unwrap();
    assert_eq!(b, Value::Blob(Vec::new()));
}

// -- Deferred types: registered but not yet implemented ----------------------

#[test]
fn deferred_decoder_present_bytes_is_not_yet_implemented() {
    let err = col(WireType::Decimal, Some(b"1.5"))
        .decoded_by::<_, String, Vec<u8>>(&DecimalTextDecoder)
        .unwrap_err();
    assert!(matches!(err, DecodeError::NotYetImplemented { .. }));
}

#[test]
fn deferred_decoder_null_still_short_circuits() {
    let got: Value<String, Vec<u8>> = col(WireType::Decimal, None)
        .decoded_by(&DecimalTextDecoder)
        .unwrap();
    assert_eq!(got, Value::Null);
}

#[test]
fn defaults_route_deferred_types_to_not_yet_implemented() {
    let types: TypeMap<PgBinary, String, Vec<u8>> = TypeMap::defaults();
    for wt in [
        WireType::Decimal,
        WireType::Timestamp,
        WireType::TimestampTz,
        WireType::Date,
        WireType::Time,
        WireType::Interval,
        WireType::Json,
        WireType::Jsonb,
    ] {
        // Registered (not NoDecoderForType) but returns NotYetImplemented.
        let err = types.decode(col(wt, Some(b"x"))).unwrap_err();
        assert!(
            matches!(err, DecodeError::NotYetImplemented { .. }),
            "wire type {wt:?} should be registered as not-yet-implemented, got {err:?}"
        );
        // NULL still short-circuits through the deferred decoder.
        assert_eq!(types.decode(col(wt, None)).unwrap(), Value::Null);
    }
}

#[test]
fn uuid_16_raw_bytes() {
    let u: Value<String, Vec<u8>> = col(WireType::Uuid, Some(&UUID_BYTES))
        .decoded_by(&UuidBlob16Decoder)
        .unwrap();
    assert_eq!(u, Value::Blob(UUID_BYTES.to_vec()));
}

// -- NULL short-circuit ------------------------------------------------------

#[test]
fn null_raw_yields_null_for_every_v1_type() {
    for wt in [
        WireType::Bool,
        WireType::Int,
        WireType::Real,
        WireType::Text,
        WireType::Bytes,
        WireType::Uuid,
    ] {
        let types: TypeMap<PgBinary, String, Vec<u8>> = TypeMap::defaults();
        let got = types.decode(col(wt, None)).unwrap();
        assert_eq!(got, Value::Null, "wire type {wt:?} null mismatch");
    }
}

// -- Malformed input errors, never panic -------------------------------------

#[test]
fn int_wrong_byte_count_errors() {
    let err = col(WireType::Int, Some(&[0x00, 0x00, 0x00]))
        .decoded_by::<_, String, Vec<u8>>(&IntDecoder)
        .unwrap_err();
    assert!(matches!(err, DecodeError::WrongPayloadKind { .. }));
}

#[test]
fn real_wrong_byte_count_errors() {
    let err = col(WireType::Real, Some(&[0, 0, 0, 0, 0]))
        .decoded_by::<_, String, Vec<u8>>(&RealDecoder)
        .unwrap_err();
    assert!(matches!(err, DecodeError::WrongPayloadKind { .. }));
}

#[test]
fn uuid_wrong_length_errors() {
    let fifteen = [0u8; 15];
    let err = col(WireType::Uuid, Some(&fifteen))
        .decoded_by::<_, String, Vec<u8>>(&UuidBlob16Decoder)
        .unwrap_err();
    assert!(matches!(
        err,
        DecodeError::InvalidUuid { source_len: 15, .. }
    ));
}

#[test]
fn text_non_utf8_errors() {
    let err = col(WireType::Text, Some(&[0xFF, 0xFE, 0xFD]))
        .decoded_by::<_, String, Vec<u8>>(&TextDecoder)
        .unwrap_err();
    assert!(matches!(err, DecodeError::InvalidUtf8 { .. }));
}

#[test]
fn bool_invalid_byte_errors() {
    let err = col(WireType::Bool, Some(&[0x02]))
        .decoded_by::<_, String, Vec<u8>>(&BoolDecoder)
        .unwrap_err();
    assert!(matches!(err, DecodeError::WrongPayloadKind { .. }));
}

// -- TypeMap dispatch --------------------------------------------------------

#[test]
fn defaults_route_v1_types() {
    let types: TypeMap<PgBinary, String, Vec<u8>> = TypeMap::defaults();

    assert_eq!(
        types.decode(col(WireType::Bool, Some(&[0x01]))).unwrap(),
        Value::Integer(1)
    );
    assert_eq!(
        types
            .decode(col(WireType::Int, Some(&[0, 0, 0, 42])))
            .unwrap(),
        Value::Integer(42)
    );
    assert_eq!(
        types
            .decode(col(WireType::Real, Some(&3.5_f64.to_be_bytes())))
            .unwrap(),
        Value::Real(3.5)
    );
    assert_eq!(
        types.decode(col(WireType::Text, Some(b"hi"))).unwrap(),
        Value::Text(String::from("hi"))
    );
    assert_eq!(
        types
            .decode(col(WireType::Bytes, Some(&[0x01, 0x02])))
            .unwrap(),
        Value::Blob(alloc::vec![0x01, 0x02])
    );
}

#[test]
fn defaults_uuid_routes_to_blob16() {
    let types: TypeMap<PgBinary, String, Vec<u8>> = TypeMap::defaults();
    let got = types
        .decode(col(WireType::Uuid, Some(&UUID_BYTES)))
        .unwrap();
    assert_eq!(got, Value::Blob(UUID_BYTES.to_vec()));
}

#[test]
fn wire_source_reports_payload_metadata() {
    use sqlite_diff_rs::WireSource;
    let p = col(WireType::Uuid, None);
    assert_eq!(PgBinary::wire_type(&p), WireType::Uuid);
    assert_eq!(PgBinary::column_name(&p), "c");
}
