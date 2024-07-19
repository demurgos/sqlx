use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::postgres::type_info::PgBuiltinType;
use crate::postgres::types::array_compatible;
use crate::postgres::{
    LazyPgTypeInfo, PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueFormat, PgValueRef,
    Postgres,
};
use crate::types::{Json, Type};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as JsonRawValue;
use serde_json::Value as JsonValue;

// <https://www.postgresql.org/docs/12/datatype-json.html>

// In general, most applications should prefer to store JSON data as jsonb,
// unless there are quite specialized needs, such as legacy assumptions
// about ordering of object keys.

impl<T> Type<Postgres> for Json<T> {
    fn type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::JSONB
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        [PgBuiltinType::Json.oid(), PgBuiltinType::Jsonb.oid()].contains(&ty.oid())
    }
}

impl<T> PgHasArrayType for Json<T> {
    fn array_type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::JSONB_ARRAY
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        array_compatible::<Json<T>>(ty)
    }
}

impl PgHasArrayType for JsonValue {
    fn array_type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::JSONB_ARRAY
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        array_compatible::<JsonValue>(ty)
    }
}

impl PgHasArrayType for JsonRawValue {
    fn array_type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::JSONB_ARRAY
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        array_compatible::<JsonRawValue>(ty)
    }
}

impl<'q, T> Encode<'q, Postgres> for Json<T>
where
    T: Serialize,
{
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        // we have a tiny amount of dynamic behavior depending if we are resolved to be JSON
        // instead of JSONB
        buf.patch(|buf, ty: &PgTypeInfo| {
            if ty.oid() == PgBuiltinType::Json.oid() || ty.oid() == PgBuiltinType::JsonArray.oid() {
                buf[0] = b' ';
            }
        });

        // JSONB version (as of 2020-03-20)
        buf.push(1);

        // the JSON data written to the buffer is the same regardless of parameter type
        serde_json::to_writer(&mut **buf, &self.0)
            .expect("failed to serialize to JSON for encoding on transmission to the database");

        IsNull::No
    }
}

impl<'r, T: 'r> Decode<'r, Postgres> for Json<T>
where
    T: Deserialize<'r>,
{
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let mut buf = value.as_bytes()?;

        if value.format() == PgValueFormat::Binary
            && value.type_info.oid() == PgBuiltinType::Jsonb.oid()
        {
            assert_eq!(
                buf[0], 1,
                "unsupported JSONB format version {}; please open an issue",
                buf[0]
            );

            buf = &buf[1..];
        }

        serde_json::from_slice(buf).map(Json).map_err(Into::into)
    }
}
