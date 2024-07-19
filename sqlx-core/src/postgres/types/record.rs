use bytes::Buf;

use crate::decode::Decode;
use crate::encode::Encode;
use crate::error::{mismatched_types, BoxDynError};
use crate::postgres::catalog::{LocalPgCatalogHandle, PgTypeRef};
use crate::postgres::type_info::{PgBuiltinType, PgType, PgTypeKind};
use crate::postgres::type_info2::LazyPgType;
use crate::postgres::types::Oid;
use crate::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueFormat, PgValueRef, Postgres};
use crate::type_info::TypeInfo;
use crate::types::Type;

#[doc(hidden)]
pub struct PgRecordEncoder<'a> {
    buf: &'a mut PgArgumentBuffer,
    off: usize,
    num: u32,
}

impl<'a> PgRecordEncoder<'a> {
    #[doc(hidden)]
    pub fn new(buf: &'a mut PgArgumentBuffer) -> Self {
        let off = buf.len();

        // reserve space for a field count
        buf.extend(&(0_u32).to_be_bytes());

        Self { buf, off, num: 0 }
    }

    #[doc(hidden)]
    pub fn finish(&mut self) {
        // fill in the record length
        self.buf[self.off..(self.off + 4)].copy_from_slice(&self.num.to_be_bytes());
    }

    #[doc(hidden)]
    pub fn encode<'q, T>(&mut self, value: T) -> &mut Self
    where
        'a: 'q,
        T: Encode<'q, Postgres> + Type<Postgres>,
    {
        let ty = value.produces().unwrap_or_else(T::type_info);

        match ty.0 {
            LazyPgType::Ref(PgTypeRef::Name(name)) => {
                // push a hole for this type ID
                // to be filled in on query execution
                self.buf.patch_type_by_name(&name);
            }
            LazyPgType::Ref(PgTypeRef::Oid(oid)) | LazyPgType::Fetched(PgType { oid, .. }) => {
                // write type id
                self.buf.extend(&oid.0.to_be_bytes());
            }
        }

        self.buf.encode(value);
        self.num += 1;

        self
    }
}

#[doc(hidden)]
pub struct PgRecordDecoder<'r> {
    buf: &'r [u8],
    catalog: LocalPgCatalogHandle,
    typ: PgTypeInfo,
    fmt: PgValueFormat,
    ind: usize,
}

impl<'r> PgRecordDecoder<'r> {
    #[doc(hidden)]
    pub fn new(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let fmt = value.format();
        let mut buf = value.as_bytes()?;
        let typ = value.type_info;

        match fmt {
            PgValueFormat::Binary => {
                let _len = buf.get_u32();
            }

            PgValueFormat::Text => {
                // remove the enclosing `(` .. `)`
                buf = &buf[1..(buf.len() - 1)];
            }
        }

        Ok(Self {
            buf,
            fmt,
            catalog: value.catalog.clone(),
            typ,
            ind: 0,
        })
    }

    #[doc(hidden)]
    pub fn try_decode<T>(&mut self) -> Result<T, BoxDynError>
    where
        T: for<'a> Decode<'a, Postgres> + Type<Postgres>,
    {
        if self.buf.is_empty() {
            return Err(format!("no field `{0}` found on record", self.ind).into());
        }

        match self.fmt {
            PgValueFormat::Binary => {
                let element_type_oid = Oid(self.buf.get_u32());
                let element_type: PgTypeInfo = match self.typ.0.kind() {
                    PgTypeKind::Simple if self.typ.oid() == PgBuiltinType::Record.oid() => {
                        // Standard `RECORD` type
                        let ty = self
                            .catalog
                            .read()
                            .resolve_type_info(&PgTypeRef::Oid(element_type_oid))
                            .map_err(|e| {
                                BoxDynError::from(format!(
                                    "unresolved record element type at index {}: {}",
                                    self.ind, e
                                ))
                            })?;
                        ty
                    }

                    PgTypeKind::Composite(composite) => {
                        // User-defined composite type
                        let ty = composite.fields[self.ind].1.get();
                        if ty.oid() != element_type_oid {
                            return Err("unexpected mismatch of composite type information".into());
                        }

                        ty
                    }

                    _ => {
                        return Err(
                            "unexpected non-composite type being decoded as a composite type"
                                .into(),
                        );
                    }
                };

                self.ind += 1;

                if !element_type.is_null() && !T::compatible(&element_type) {
                    return Err(mismatched_types::<Postgres, T>(&element_type));
                }

                T::decode(PgValueRef::get(
                    &mut self.buf,
                    self.fmt,
                    self.catalog.clone(),
                    element_type,
                ))
            }

            PgValueFormat::Text => {
                let mut element = String::new();
                let mut quoted = false;
                let mut in_quotes = false;
                let mut in_escape = false;
                let mut prev_ch = '\0';

                while !self.buf.is_empty() {
                    let ch = self.buf.get_u8() as char;
                    match ch {
                        _ if in_escape => {
                            element.push(ch);
                            in_escape = false;
                        }

                        '"' if in_quotes => {
                            in_quotes = false;
                        }

                        '"' => {
                            in_quotes = true;
                            quoted = true;

                            if prev_ch == '"' {
                                element.push('"')
                            }
                        }

                        '\\' if !in_escape => {
                            in_escape = true;
                        }

                        ',' if !in_quotes => break,

                        _ => {
                            element.push(ch);
                        }
                    }
                    prev_ch = ch;
                }

                let buf = if element.is_empty() && !quoted {
                    // completely empty input means NULL
                    None
                } else {
                    Some(element.as_bytes())
                };

                // NOTE: we do not call [`accepts`] or give a chance to from a user as
                //       TEXT sequences are not strongly typed

                // NOTE: We pass `UNKNOWN` as the type because we don't have a reasonable value
                //       we could use.
                let type_info = self
                    .catalog
                    .read()
                    .resolve_type_info(&PgTypeRef::Oid(PgBuiltinType::Unknown.oid()))
                    .expect("(BUG) Local catalog is missing the postgres `UNKNOWN` type");

                T::decode(PgValueRef {
                    value: buf,
                    row: None,
                    catalog: self.catalog.clone(),
                    type_info,
                    format: self.fmt,
                })
            }
        }
    }
}
