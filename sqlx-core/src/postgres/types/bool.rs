use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::postgres::type_info2::PgBuiltinType;
use crate::postgres::{
    LazyPgTypeInfo, PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueFormat, PgValueRef,
    Postgres,
};
use crate::types::Type;

impl Type<Postgres> for bool {
    fn type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::BOOL
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        ty.oid() == PgBuiltinType::Bool.oid()
    }
}

impl PgHasArrayType for bool {
    fn array_type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::BOOL_ARRAY
    }

    fn array_compatible(ty: &PgTypeInfo) -> bool {
        ty.oid() == PgBuiltinType::BoolArray.oid()
    }
}

impl Encode<'_, Postgres> for bool {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        buf.push(*self as u8);

        IsNull::No
    }
}

impl Decode<'_, Postgres> for bool {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            PgValueFormat::Binary => value.as_bytes()?[0] != 0,

            PgValueFormat::Text => match value.as_str()? {
                "t" => true,
                "f" => false,

                s => {
                    return Err(format!("unexpected value {:?} for boolean", s).into());
                }
            },
        })
    }
}
