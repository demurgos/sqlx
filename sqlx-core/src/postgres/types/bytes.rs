use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::postgres::type_info2::PgBuiltinType;
use crate::postgres::{
    LazyPgTypeInfo, PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueFormat, PgValueRef,
    Postgres,
};
use crate::types::Type;

impl PgHasArrayType for u8 {
    fn array_type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::BYTEA
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        ty.oid() == PgBuiltinType::Bytea.oid()
    }
}

impl PgHasArrayType for &'_ [u8] {
    fn array_type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::BYTEA_ARRAY
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        ty.oid() == PgBuiltinType::ByteaArray.oid()
    }
}

impl PgHasArrayType for Vec<u8> {
    fn array_type_info() -> LazyPgTypeInfo {
        <[&[u8]] as Type<Postgres>>::type_info()
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        <[&[u8]] as Type<Postgres>>::compatible(ty)
    }
}

impl Encode<'_, Postgres> for &'_ [u8] {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        buf.extend_from_slice(self);

        IsNull::No
    }
}

impl Encode<'_, Postgres> for Vec<u8> {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        <&[u8] as Encode<Postgres>>::encode(self, buf)
    }
}

impl<'r> Decode<'r, Postgres> for &'r [u8] {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.format() {
            PgValueFormat::Binary => value.as_bytes(),
            PgValueFormat::Text => {
                Err("unsupported decode to `&[u8]` of BYTEA in a simple query; use a prepared query or decode to `Vec<u8>`".into())
            }
        }
    }
}

impl Decode<'_, Postgres> for Vec<u8> {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            PgValueFormat::Binary => value.as_bytes()?.to_owned(),
            PgValueFormat::Text => {
                // BYTEA is formatted as \x followed by hex characters
                hex::decode(&value.as_str()?[2..])?
            }
        })
    }
}
