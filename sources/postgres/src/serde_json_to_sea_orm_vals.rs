use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use sea_orm::{
    ColumnTrait, DbErr,
    sea_query::{ArrayType, ColumnType},
};
use uuid::Uuid;

type SValue = sea_orm::Value;

pub fn json_to_value_for_column<C: ColumnTrait>(
    col: &C,
    v: serde_json::Value,
) -> Result<SValue, DbErr> {
    let def = col.def();
    let ty = def.get_column_type();

    json_to_value_of_type(ty, v)
}

pub fn json_to_value_for_column_arr<C: ColumnTrait>(
    col: &C,
    vs: Vec<serde_json::Value>,
) -> Result<Vec<SValue>, DbErr> {
    let def = col.def();
    let ty = def.get_column_type();

    vs.into_iter()
        .map(|v| json_to_value_of_type(ty, v))
        .collect()
}

fn column_type_to_array_type(ty: &ColumnType) -> Result<ArrayType, DbErr> {
    let arr_type = match ty {
        ColumnType::Char(_) => ArrayType::Char,
        ColumnType::String(_) | ColumnType::Text => ArrayType::String,
        ColumnType::Blob => ArrayType::Bytes,
        ColumnType::TinyInteger => ArrayType::TinyInt,
        ColumnType::SmallInteger => ArrayType::SmallInt,
        ColumnType::Integer => ArrayType::Int,
        ColumnType::BigInteger => ArrayType::BigInt,
        ColumnType::TinyUnsigned => ArrayType::TinyUnsigned,
        ColumnType::SmallUnsigned => ArrayType::SmallUnsigned,
        ColumnType::Unsigned => ArrayType::Unsigned,
        ColumnType::BigUnsigned => ArrayType::BigUnsigned,
        ColumnType::Float => ArrayType::Float,
        ColumnType::Double => ArrayType::Double,
        ColumnType::Decimal(_) => ArrayType::Decimal,
        ColumnType::DateTime => ArrayType::ChronoDateTime,
        ColumnType::Timestamp => ArrayType::ChronoDateTime,
        ColumnType::TimestampWithTimeZone => ArrayType::ChronoDateTimeWithTimeZone,
        ColumnType::Time => ArrayType::ChronoTime,
        ColumnType::Date => ArrayType::ChronoDate,
        ColumnType::Binary(_) => ArrayType::Bytes,
        ColumnType::VarBinary(_) => ArrayType::Bytes,
        ColumnType::Bit(_) => ArrayType::Unsigned,
        ColumnType::VarBit(_) => ArrayType::Unsigned,
        ColumnType::Boolean => ArrayType::Bool,
        ColumnType::Json => ArrayType::Json,
        ColumnType::JsonBinary => ArrayType::Json,
        ColumnType::Uuid => ArrayType::Uuid,
        ColumnType::Enum { .. } => ArrayType::String,
        ColumnType::Array(column_type) => column_type_to_array_type(column_type)?,
        err => return Err(DbErr::Custom(format!("Unsupported error type {err:?}"))),
    };

    Ok(arr_type)
}

fn json_to_value_of_type(ty: &ColumnType, v: serde_json::Value) -> Result<SValue, DbErr> {
    use serde_json::Value as J;
    match ty {
        // Booleans
        ColumnType::Boolean => match v {
            J::Bool(b) => Ok(SValue::Bool(Some(b))),
            J::Null => Ok(SValue::Bool(None).as_null()),
            _ => Err(type_err("bool", &v)),
        },

        // Integers (Postgres has no unsigned; map to signed)
        ColumnType::TinyInteger => as_i64_in_range(v, i8::MIN as i64, i8::MAX as i64)
            .map(|n| SValue::TinyInt(Some(n as i8))),
        ColumnType::SmallInteger => as_i64_in_range(v, i16::MIN as i64, i16::MAX as i64)
            .map(|n| SValue::SmallInt(Some(n as i16))),
        ColumnType::Integer => as_i64_in_range(v, i32::MIN as i64, i32::MAX as i64)
            .map(|n| SValue::Int(Some(n as i32))),
        ColumnType::BigInteger => as_i64(v).map(|n| SValue::BigInt(Some(n))),

        // Floating / Decimal
        ColumnType::Float => as_f64(v).map(|f| SValue::Float(Some(f as f32))),
        ColumnType::Double => as_f64(v).map(|f| SValue::Double(Some(f))),
        ColumnType::Decimal(_prec) => match v {
            J::Number(n) => {
                let s = n.to_string();
                Decimal::from_str_exact(&s)
                    .map(|d| SValue::Decimal(Some(Box::new(d))))
                    .map_err(|_| DbErr::Custom(format!("Expected decimal, got {}", s)))
            }
            J::String(s) => Decimal::from_str_exact(&s)
                .map(|d| SValue::Decimal(Some(Box::new(d))))
                .map_err(|_| DbErr::Custom(format!("Expected decimal, got {}", s))),
            J::Null => Ok(SValue::Decimal(None)),
            _ => Err(type_err("decimal", &v)),
        },

        // Text-ish
        ColumnType::Char(_) | ColumnType::String(_) | ColumnType::Text => match v {
            J::String(s) => Ok(SValue::String(Some(Box::new(s)))),
            J::Null => Ok(SValue::String(None)),
            _ => Err(type_err("string", &v)),
        },

        // UUID
        ColumnType::Uuid => match v {
            J::String(s) => Uuid::parse_str(&s)
                .map(|u| SValue::Uuid(Some(Box::new(u))))
                .map_err(|_| DbErr::Custom(format!("Invalid UUID: {}", s))),
            J::Null => Ok(SValue::Uuid(None)),
            _ => Err(type_err("uuid string", &v)),
        },

        // JSON(B)
        ColumnType::Json => Ok(SValue::Json(Some(Box::new(v)))),
        ColumnType::JsonBinary => Ok(SValue::Json(Some(Box::new(v)))),

        // Dates / times (expect RFC3339 for datetime/timestamp; plain for date/time)
        ColumnType::Date => match v {
            J::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .map(|d| SValue::ChronoDate(Some(Box::new(d))))
                .map_err(|_| DbErr::Custom(format!("Invalid date (YYYY-MM-DD): {}", s))),
            J::Null => Ok(SValue::ChronoDate(None)),
            _ => Err(type_err("date string (YYYY-MM-DD)", &v)),
        },
        ColumnType::Time => match v {
            J::String(s) => NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
                .map(|t| SValue::ChronoTime(Some(Box::new(t))))
                .map_err(|_| DbErr::Custom(format!("Invalid time (HH:MM:SS[.ffff]): {}", s))),
            J::Null => Ok(SValue::ChronoTime(None)),
            _ => Err(type_err("time string", &v)),
        },
        ColumnType::Timestamp | ColumnType::DateTime => match v {
            J::String(s) => {
                // Accept RFC3339 or naive "YYYY-MM-DD HH:MM:SS"
                chrono::DateTime::parse_from_rfc3339(&s)
                    .map(|dt| dt.naive_utc())
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f"))
                    .map(|ndt| SValue::ChronoDateTime(Some(Box::new(ndt))))
                    .map_err(|_| DbErr::Custom(format!("Invalid datetime: {}", s)))
            }
            J::Null => Ok(SValue::ChronoDateTime(None)),
            _ => Err(type_err("datetime string", &v)),
        },
        ColumnType::TimestampWithTimeZone => match v {
            J::String(s) => {
                // Accept RFC3339 or naive "YYYY-MM-DD HH:MM:SS"
                chrono::DateTime::parse_from_rfc3339(&s)
                    .map(|ndt| SValue::ChronoDateTimeWithTimeZone(Some(Box::new(ndt))))
                    .map_err(|_| DbErr::Custom(format!("Invalid datetime: {}", s)))
            }
            J::Null => Ok(SValue::ChronoDateTime(None)),
            _ => Err(type_err("datetime string", &v)),
        },
        // Bytea
        ColumnType::Binary(_) | ColumnType::VarBinary(_) => match v {
            J::String(s) => {
                // Expect base64 by convention; adjust if you prefer hex
                let bytes = BASE64_STANDARD
                    .decode(s)
                    .map_err(|_| DbErr::Custom("Invalid base64 for bytea".into()))?;
                Ok(SValue::Bytes(Some(Box::new(bytes))))
            }
            J::Null => Ok(SValue::Bytes(None)),
            _ => Err(type_err("base64 string", &v)),
        },

        // Arrays (Postgres): require JSON array, convert elements by inner type
        ColumnType::Array(inner) => match v {
            J::Array(arr) => {
                let mut out = Vec::with_capacity(arr.len());
                for el in arr {
                    out.push(json_to_value_of_type(inner, el)?);
                }
                Ok(SValue::Array(
                    column_type_to_array_type(ty)?,
                    Some(Box::new(out)),
                ))
            }
            J::Null => Ok(SValue::Array(
                column_type_to_array_type(ty)?,
                Some(Box::new(Vec::new())),
            )),
            _ => Err(type_err("array", &v)),
        },

        // Enums: treat as strings (your entities map DB enum <-> Rust enum elsewhere)
        ColumnType::Enum { .. } => match v {
            J::String(s) => Ok(SValue::String(Some(Box::new(s)))),
            J::Null => Ok(SValue::String(None)),
            _ => Err(type_err("enum string", &v)),
        },

        // Catch-alls: add more as you use them (e.g., `Money`, `Cidr`, etc., if modeled)
        other => Err(DbErr::Custom(format!(
            "Unsupported column type in filter: {:?}",
            other
        ))),
    }
}

fn type_err(expected: &str, got: &serde_json::Value) -> DbErr {
    DbErr::Custom(format!("Expected {expected}, got {got}"))
}

fn as_i64(v: serde_json::Value) -> Result<i64, DbErr> {
    match v {
        serde_json::Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| DbErr::Custom(format!("Expected integer, got {n}"))),
        serde_json::Value::String(s) => s
            .parse::<i64>()
            .map_err(|_| DbErr::Custom(format!("Expected integer, got {s}"))),
        serde_json::Value::Null => Err(DbErr::Custom("null not allowed here".into())),
        _ => Err(DbErr::Custom(format!("Expected integer, got {v}"))),
    }
}
fn as_i64_in_range(v: serde_json::Value, min: i64, max: i64) -> Result<i64, DbErr> {
    let n = as_i64(v)?;
    if n < min || n > max {
        return Err(DbErr::Custom(format!(
            "Integer {n} out of range [{min},{max}]"
        )));
    }
    Ok(n)
}
fn as_f64(v: serde_json::Value) -> Result<f64, DbErr> {
    match v {
        serde_json::Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| DbErr::Custom(format!("Expected number, got {n}"))),
        serde_json::Value::String(s) => s
            .parse::<f64>()
            .map_err(|_| DbErr::Custom(format!("Expected number, got {s}"))),
        serde_json::Value::Null => Err(DbErr::Custom("null not allowed here".into())),
        _ => Err(DbErr::Custom(format!("Expected number, got {v}"))),
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    use base64::{Engine, prelude::BASE64_STANDARD};
    use rust_decimal::Decimal;
    use sea_orm::entity::prelude::*;
    use sea_orm::sea_query::ArrayType;
    use serde_json::json;
    use uuid::Uuid;

    // --- Test entity (Postgres-only) -------------------------------------------------

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "t_conv")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i32,

        // scalars
        pub b: Option<bool>,
        pub i: Option<i32>,
        pub bi: Option<i64>,
        pub f: Option<f32>,
        pub d: Option<f64>,
        pub dec_: Option<Decimal>,
        pub s: Option<String>,
        #[sea_orm(column_type = "Text")]
        pub txt: Option<String>,

        // chrono
        pub date_: Option<chrono::NaiveDate>,
        pub time_: Option<chrono::NaiveTime>,
        pub dt_: Option<chrono::NaiveDateTime>,
        // SeaORM represents timestamp with tz as NaiveDateTime at the value layer too
        pub dttz_: Option<chrono::DateTime<chrono::Utc>>,

        // uuid / json / bytes
        pub uid: Option<Uuid>,
        pub js: Option<serde_json::Value>,
        pub bin: Option<Vec<u8>>,

        // postgres arrays (we only need correct ColumnType from macro)
        pub int_arr: Option<Vec<i32>>,
        pub str_arr: Option<Vec<String>>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}

    // --- tiny inspectors for SValue --------------------------------------------------

    fn is_bool(v: &SValue, expected: Option<bool>) -> bool {
        matches!(v, SValue::Bool(b) if *b == expected)
    }
    fn is_int(v: &SValue, expected: i32) -> bool {
        matches!(v, SValue::Int(Some(x)) if *x == expected)
    }
    fn is_bigint(v: &SValue, expected: i64) -> bool {
        matches!(v, SValue::BigInt(Some(x)) if *x == expected)
    }
    fn is_float(v: &SValue, expected: f32) -> bool {
        matches!(v, SValue::Float(Some(x)) if (*x - expected).abs() < 1e-6)
    }
    fn is_double(v: &SValue, expected: f64) -> bool {
        matches!(v, SValue::Double(Some(x)) if (*x - expected).abs() < 1e-12)
    }
    fn is_string(v: &SValue, expected: &str) -> bool {
        matches!(v, SValue::String(Some(s)) if s.as_str() == expected)
    }
    fn is_uuid(v: &SValue, expected: Uuid) -> bool {
        matches!(v, SValue::Uuid(Some(u)) if **u == expected)
    }
    fn is_json(v: &SValue, pred: impl Fn(&serde_json::Value) -> bool) -> bool {
        matches!(v, SValue::Json(Some(j)) if pred(j))
    }
    fn is_date(v: &SValue, expected: &str) -> bool {
        let nd = chrono::NaiveDate::parse_from_str(expected, "%Y-%m-%d").unwrap();
        matches!(v, SValue::ChronoDate(Some(d)) if **d == nd)
    }
    fn is_time(v: &SValue, expected: &str) -> bool {
        let nt = chrono::NaiveTime::parse_from_str(expected, "%H:%M:%S%.f").unwrap();
        matches!(v, SValue::ChronoTime(Some(t)) if **t == nt)
    }
    fn is_datetime(v: &SValue, expected_fmt: Option<&str>) -> bool {
        match (v, expected_fmt) {
            (SValue::ChronoDateTime(Some(_)), None) => true,
            (SValue::ChronoDateTime(Some(dt)), Some(fmt)) => {
                let want =
                    chrono::NaiveDateTime::parse_from_str(fmt, "%Y-%m-%d %H:%M:%S%.f").unwrap();
                **dt == want
            }
            _ => false,
        }
    }
    fn is_datetime_with_tz(v: &SValue, expected_dtz_str: Option<&str>) -> bool {
        match (v, expected_dtz_str) {
            (SValue::ChronoDateTimeWithTimeZone(Some(_)), None) => true,
            (SValue::ChronoDateTimeWithTimeZone(Some(dt)), Some(fmt)) => {
                let want = chrono::DateTime::parse_from_rfc3339(fmt).unwrap();
                **dt == want
            }
            _ => false,
        }
    }
    fn is_bytes(v: &SValue, expected: &[u8]) -> bool {
        matches!(v, SValue::Bytes(Some(b)) if b.as_slice() == expected)
    }
    fn is_array(v: &SValue, expect_ty: ArrayType, len: usize) -> bool {
        match v {
            SValue::Array(ty, Some(vals)) => *ty == expect_ty && vals.len() == len,
            SValue::Array(ty, None) => *ty == expect_ty && len == 0,
            _ => false,
        }
    }

    // --- tests ----------------------------------------------------------------------

    #[test]
    fn bool_ok() {
        let v = json_to_value_for_column(&Column::B, json!(true)).unwrap();
        assert!(is_bool(&v, Some(true)));

        let v = json_to_value_for_column(&Column::B, serde_json::Value::Null).unwrap();
        assert!(matches!(v, SValue::Bool(None)));
    }

    #[test]
    fn ints_ok() {
        assert!(is_int(
            &json_to_value_for_column(&Column::I, json!(123)).unwrap(),
            123
        ));
        assert!(is_bigint(
            &json_to_value_for_column(&Column::Bi, json!(9_007_199_254_740_991_i64)).unwrap(),
            9_007_199_254_740_991
        ));
    }

    #[test]
    fn floats_ok() {
        assert!(is_float(
            &json_to_value_for_column(&Column::F, json!(1.25)).unwrap(),
            1.25
        ));
        assert!(is_double(
            &json_to_value_for_column(&Column::D, json!(3.5)).unwrap(),
            3.5
        ));
    }

    #[test]
    fn decimal_ok() {
        let v = json_to_value_for_column(&Column::Dec, json!("123.4500")).unwrap();
        match v {
            SValue::Decimal(Some(d)) => assert_eq!(d.normalize().to_string(), "123.45"),
            _ => panic!("expected decimal"),
        }
    }

    #[test]
    fn strings_ok() {
        assert!(is_string(
            &json_to_value_for_column(&Column::S, json!("hi")).unwrap(),
            "hi"
        ));
        assert!(is_string(
            &json_to_value_for_column(&Column::Txt, json!("lorem")).unwrap(),
            "lorem"
        ));
    }

    #[test]
    fn uuid_ok() {
        let u = Uuid::new_v4();
        assert!(is_uuid(
            &json_to_value_for_column(&Column::Uid, json!(u.to_string())).unwrap(),
            u
        ));
        assert!(json_to_value_for_column(&Column::Uid, json!("not-a-uuid")).is_err());
    }

    #[test]
    fn json_ok() {
        let v = json_to_value_for_column(&Column::Js, json!({"a": 1})).unwrap();
        assert!(is_json(&v, |j| j["a"] == 1));
    }

    #[test]
    fn dates_times_ok() {
        assert!(is_date(
            &json_to_value_for_column(&Column::Date, json!("2024-12-31")).unwrap(),
            "2024-12-31"
        ));
        assert!(is_time(
            &json_to_value_for_column(&Column::Time, json!("23:59:59.123")).unwrap(),
            "23:59:59.123"
        ));

        // naive
        assert!(is_datetime(
            &json_to_value_for_column(&Column::Dt, json!("2024-12-31 23:59:59.123")).unwrap(),
            Some("2024-12-31 23:59:59.123")
        ));
        // RFC3339 → naive_utc
        assert!(is_datetime(
            &json_to_value_for_column(&Column::Dt, json!("2024-12-31T23:59:59Z")).unwrap(),
            None
        ));
        // tz column accepts RFC3339 too
        assert!(is_datetime_with_tz(
            &json_to_value_for_column(&Column::Dttz, json!("2024-12-31T23:59:59+02:00")).unwrap(),
            None
        ));
    }

    #[test]
    fn bytes_ok() {
        let data = b"\x00\x01\x02hello";
        let b64 = BASE64_STANDARD.encode(data);
        assert!(is_bytes(
            &json_to_value_for_column(&Column::Bin, json!(b64)).unwrap(),
            data
        ));
        assert!(json_to_value_for_column(&Column::Bin, json!("!!!")).is_err());
    }

    #[test]
    fn arrays_ok() {
        let v = json_to_value_for_column(&Column::IntArr, json!([1, 2, 3])).unwrap();
        assert!(is_array(&v, ArrayType::Int, 3));

        let v = json_to_value_for_column(&Column::StrArr, json!(["a", "b"])).unwrap();
        assert!(is_array(&v, ArrayType::String, 2));

        // null array -> empty Some(Vec::new()) in your impl
        assert!(json_to_value_for_column(&Column::IntArr, json!(null)).is_ok());
        // wrong type
        assert!(json_to_value_for_column(&Column::IntArr, json!("oops")).is_err());
    }

    #[test]
    fn batch_arr_ok() {
        let out =
            json_to_value_for_column_arr(&Column::I, vec![json!(1), json!(2), json!(3)]).unwrap();
        assert_eq!(out.len(), 3);
        assert!(is_int(&out[0], 1));
        assert!(is_int(&out[1], 2));
        assert!(is_int(&out[2], 3));
    }
}
