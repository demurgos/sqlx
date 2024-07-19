use crate::decode::Decode;
use crate::error::BoxDynError;
use crate::postgres::type_info2::PgBuiltinType;
use crate::postgres::{LazyPgTypeInfo, PgTypeInfo, PgValueRef, Postgres};
use crate::types::Type;

impl Type<Postgres> for () {
    fn type_info() -> LazyPgTypeInfo {
        LazyPgTypeInfo::VOID
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        // RECORD is here so we can support the empty tuple
        [PgBuiltinType::Void.oid(), PgBuiltinType::Record.oid()].contains(&ty.oid())
    }
}

impl<'r> Decode<'r, Postgres> for () {
    fn decode(_value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(())
    }
}
