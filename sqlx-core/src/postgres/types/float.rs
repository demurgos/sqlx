use byteorder::{BigEndian, ByteOrder};

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::postgres::type_info2::PgBuiltinType;
use crate::postgres::{
    LazyPgTypeInfo, PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueFormat, PgValueRef,
    Postgres,
};
use crate::types::Type;

impl Type<Postgres> for f32 {
    fn type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::FLOAT4
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        ty.oid() == PgBuiltinType::Float4.oid()
    }
}

impl PgHasArrayType for f32 {
    fn array_type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::FLOAT4_ARRAY
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        ty.oid() == PgBuiltinType::Float4Array.oid()
    }
}

impl Encode<'_, Postgres> for f32 {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        buf.extend(&self.to_be_bytes());

        IsNull::No
    }
}

impl Decode<'_, Postgres> for f32 {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            PgValueFormat::Binary => BigEndian::read_f32(value.as_bytes()?),
            PgValueFormat::Text => value.as_str()?.parse()?,
        })
    }
}

impl Type<Postgres> for f64 {
    fn type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::FLOAT8
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        ty.oid() == PgBuiltinType::Float8.oid()
    }
}

impl PgHasArrayType for f64 {
    fn array_type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::FLOAT8_ARRAY
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        ty.oid() == PgBuiltinType::Float8Array.oid()
    }
}

impl Encode<'_, Postgres> for f64 {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        buf.extend(&self.to_be_bytes());

        IsNull::No
    }
}

impl Decode<'_, Postgres> for f64 {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            PgValueFormat::Binary => BigEndian::read_f64(value.as_bytes()?),
            PgValueFormat::Text => value.as_str()?.parse()?,
        })
    }
}
