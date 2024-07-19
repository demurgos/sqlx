use crate::error::Error;
use crate::ext::ustr::UStr;
use crate::postgres::catalog::{GetPgTypeError, PgTypeRef};
use crate::postgres::message::{ParameterDescription, RowDescription};
use crate::postgres::statement::PgStatementMetadata;
use crate::postgres::type_info::{PgType, PgTypeKind, PgTypeOid};
use crate::postgres::types::Oid;
use crate::postgres::{PgArguments, PgColumn, PgConnection, PgTypeInfo};
use crate::query_as::query_as;
use crate::query_scalar::{query_scalar, query_scalar_with};
use crate::types::Json;
use crate::HashMap;
use futures_core::future::BoxFuture;
use std::fmt::Write;

/// Describes the type of the `pg_type.typtype` column
///
/// See <https://www.postgresql.org/docs/13/catalog-pg-type.html>
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum TypType {
    Base,
    Composite,
    Domain,
    Enum,
    Pseudo,
    Range,
}

impl TryFrom<u8> for TypType {
    type Error = ();

    fn try_from(t: u8) -> Result<Self, Self::Error> {
        let t = match t {
            b'b' => Self::Base,
            b'c' => Self::Composite,
            b'd' => Self::Domain,
            b'e' => Self::Enum,
            b'p' => Self::Pseudo,
            b'r' => Self::Range,
            _ => return Err(()),
        };
        Ok(t)
    }
}

/// Describes the type of the `pg_type.typcategory` column
///
/// See <https://www.postgresql.org/docs/13/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE>
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum TypCategory {
    Array,
    Boolean,
    Composite,
    DateTime,
    Enum,
    Geometric,
    Network,
    Numeric,
    Pseudo,
    Range,
    String,
    Timespan,
    User,
    BitString,
    Unknown,
}

impl TryFrom<u8> for TypCategory {
    type Error = ();

    fn try_from(c: u8) -> Result<Self, Self::Error> {
        let c = match c {
            b'A' => Self::Array,
            b'B' => Self::Boolean,
            b'C' => Self::Composite,
            b'D' => Self::DateTime,
            b'E' => Self::Enum,
            b'G' => Self::Geometric,
            b'I' => Self::Network,
            b'N' => Self::Numeric,
            b'P' => Self::Pseudo,
            b'R' => Self::Range,
            b'S' => Self::String,
            b'T' => Self::Timespan,
            b'U' => Self::User,
            b'V' => Self::BitString,
            b'X' => Self::Unknown,
            _ => return Err(()),
        };
        Ok(c)
    }
}

impl PgConnection {
    pub(super) async fn handle_row_description(
        &mut self,
        desc: Option<RowDescription>,
        should_fetch: bool,
    ) -> Result<(Vec<PgColumn>, HashMap<UStr, usize>), Error> {
        let mut columns = Vec::new();
        let mut column_names = HashMap::new();

        let desc = if let Some(desc) = desc {
            desc
        } else {
            // no rows
            return Ok((columns, column_names));
        };

        columns.reserve(desc.fields.len());
        column_names.reserve(desc.fields.len());

        for (index, field) in desc.fields.into_iter().enumerate() {
            let name = UStr::from(field.name);

            let type_info = if should_fetch {
                self.maybe_fetch_type_info_by_oid(field.data_type_id)
                    .await?
            } else {
                self.local_catalog
                    .read()
                    .resolve_type_info(&PgTypeRef::Oid(field.data_type_id))
                    .map_err(|e| Error::TypeNotFound {
                        type_name: format!("PgType[{:?}]", e.typ),
                    })?
            };

            let column = PgColumn {
                ordinal: index,
                name: name.clone(),
                type_info,
                relation_id: field.relation_id,
                relation_attribute_no: field.relation_attribute_no,
            };

            columns.push(column);
            column_names.insert(name, index);
        }

        Ok((columns, column_names))
    }

    pub(super) async fn handle_parameter_description(
        &mut self,
        desc: ParameterDescription,
    ) -> Result<Vec<PgTypeInfo>, Error> {
        let mut params = Vec::with_capacity(desc.types.len());

        for ty in desc.types {
            params.push(self.maybe_fetch_type_info_by_oid(ty).await?);
        }

        Ok(params)
    }

    async fn maybe_fetch_type_info_by_oid(&mut self, oid: Oid) -> Result<PgTypeInfo, Error> {
        let (mut unfetched, cached) = {
            let catalog = self.local_catalog.read();
            let unfetched: Vec<PgTypeRef> = catalog.get_unfetched().cloned().collect();
            let cached = catalog.resolve_type_info(&PgTypeRef::Oid(oid));
            (unfetched, cached)
        };

        let cached = match cached {
            Ok(ty) => {
                if unfetched.is_empty() {
                    // This is the most common case:
                    // Nothing pending and the type is cached.
                    return Ok(ty);
                }
                Some(Ok(ty))
            }
            Err(e) => match &e.error {
                GetPgTypeError::Missing => {
                    if unfetched.is_empty() {
                        return Err(Error::TypeNotFound {
                            type_name: format!("PgType[oid={}]", oid),
                        });
                    }
                    Some(Err(e))
                }
                GetPgTypeError::Undeclared | GetPgTypeError::Unfetched => {
                    unfetched.push(PgTypeRef::Oid(oid));
                    None
                }
            },
        };

        // Prevent infinite recursion while reading the responses for the catalog queries.
        if !self.fetching_types {
            self.fetching_types = true;
            let res = self.fetch_declared(unfetched).await;
            self.fetching_types = false;
            if let Err(e) = res {
                return Err(e);
            }
        }

        let resolved = cached.unwrap_or_else(|| {
            self.local_catalog
                .read()
                .resolve_type_info(&PgTypeRef::Oid(oid))
        });

        match resolved {
            Ok(ty) => Ok(ty),
            Err(_) => Err(Error::TypeNotFound {
                type_name: format!("PgType[oid={}]", oid),
            }),
        }
    }

    /// Fetch all declared but never fetched types.
    async fn fetch_declared(&mut self, mut unfetched: Vec<PgTypeRef>) -> Result<(), Error> {
        while !unfetched.is_empty() {
            for ty_ref in unfetched.drain(..) {
                match self.fetch_type(&ty_ref).await {
                    Ok(ty) => self.local_catalog.write().insert_type(ty).unwrap(),
                    Err(Error::RowNotFound) => {
                        let e = Err(Error::TypeNotFound {
                            type_name: format!("PgType[{:?}]", ty_ref),
                        });
                        self.local_catalog
                            .write()
                            .flag_type_as_missing(ty_ref)
                            .unwrap();
                        return e;
                    }
                    Err(e) => return Err(e),
                }
            }
            unfetched.extend(self.local_catalog.read().get_unfetched().cloned());
        }
        Ok(())
    }

    async fn fetch_type(&mut self, ty_ref: &PgTypeRef) -> Result<PgType<PgTypeOid>, Error> {
        type Row = (Oid, String, i8, i8, Oid, Oid, Oid);
        let mut oid: Option<Oid> = None;
        let mut name: Option<&str> = None;
        match ty_ref {
            PgTypeRef::Oid(r) => oid = Some(*r),
            PgTypeRef::Name(r) => name = Some(&r),
        }
        // language=PostgreSQL
        let row: Option<Row> = query_as(
            r#"
                SELECT oid, typname, typtype, typcategory, typrelid, typelem, typbasetype
                FROM pg_catalog.pg_type
                WHERE oid = $1 OR typname ILIKE $2;
            "#,
        )
        .bind(oid)
        .bind(name)
        .fetch_optional(&mut *self)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                return Err(Error::RowNotFound);
            }
        };

        let (oid, name, typ_type, category, relation_id, element, base_type) = row;

        let typ_type = TypType::try_from(typ_type as u8);
        let category = TypCategory::try_from(category as u8);

        match (typ_type, category) {
            (Ok(TypType::Domain), _) => self.fetch_domain_by_oid(oid, base_type, name).await,

            (Ok(TypType::Base), Ok(TypCategory::Array)) => Ok(PgType {
                oid,
                name: name.into(),
                kind: PgTypeKind::Array(element),
            }),

            (Ok(TypType::Pseudo), Ok(TypCategory::Pseudo)) => Ok(PgType {
                oid,
                name: name.into(),
                kind: PgTypeKind::Pseudo,
            }),

            (Ok(TypType::Range), Ok(TypCategory::Range)) => {
                self.fetch_range_by_oid(oid, name).await
            }

            (Ok(TypType::Enum), Ok(TypCategory::Enum)) => self.fetch_enum_by_oid(oid, name).await,

            (Ok(TypType::Composite), Ok(TypCategory::Composite)) => {
                self.fetch_composite_by_oid(oid, relation_id, name).await
            }

            _ => Ok(PgType {
                oid,
                name: name.into(),
                kind: PgTypeKind::Simple,
            }),
        }
    }

    async fn fetch_enum_by_oid(
        &mut self,
        oid: Oid,
        name: String,
    ) -> Result<PgType<PgTypeOid>, Error> {
        let variants: Vec<String> = query_scalar(
            r#"
SELECT enumlabel
FROM pg_catalog.pg_enum
WHERE enumtypid = $1
ORDER BY enumsortorder
            "#,
        )
        .bind(oid)
        .fetch_all(self)
        .await?;

        Ok(PgType {
            oid,
            name: name.into(),
            kind: PgTypeKind::Enum(variants.into_boxed_slice()),
        })
    }

    fn fetch_composite_by_oid(
        &mut self,
        oid: Oid,
        relation_id: Oid,
        name: String,
    ) -> BoxFuture<'_, Result<PgType<PgTypeOid>, Error>> {
        Box::pin(async move {
            let fields: Vec<(String, Oid)> = query_as(
                r#"
SELECT attname, atttypid
FROM pg_catalog.pg_attribute
WHERE attrelid = $1
AND NOT attisdropped
AND attnum > 0
ORDER BY attnum
                "#,
            )
            .bind(relation_id)
            .fetch_all(&mut *self)
            .await?;

            Ok(PgType {
                oid,
                name: name.into(),
                kind: PgTypeKind::composite(fields),
            })
        })
    }

    fn fetch_domain_by_oid(
        &mut self,
        oid: Oid,
        base_type: Oid,
        name: String,
    ) -> BoxFuture<'_, Result<PgType<PgTypeOid>, Error>> {
        Box::pin(async move {
            Ok(PgType {
                oid,
                name: name.into(),
                kind: PgTypeKind::Domain(base_type),
            })
        })
    }

    fn fetch_range_by_oid(
        &mut self,
        oid: Oid,
        name: String,
    ) -> BoxFuture<'_, Result<PgType<PgTypeOid>, Error>> {
        Box::pin(async move {
            let element_oid: Oid = query_scalar(
                r#"
SELECT rngsubtype
FROM pg_catalog.pg_range
WHERE rngtypid = $1
                "#,
            )
            .bind(oid)
            .fetch_one(&mut *self)
            .await?;

            Ok(PgType {
                oid,
                name: name.into(),
                kind: PgTypeKind::Range(element_oid),
            })
        })
    }

    pub(crate) async fn fetch_type_id_by_name(&mut self, name: &str) -> Result<Oid, Error> {
        if let Some(oid) = self.cache_type_oid.get(name) {
            return Ok(*oid);
        }

        // language=SQL
        let (oid,): (Oid,) = query_as(
            "
SELECT oid FROM pg_catalog.pg_type WHERE typname ILIKE $1
                ",
        )
        .bind(name)
        .fetch_optional(&mut *self)
        .await?
        .ok_or_else(|| Error::TypeNotFound {
            type_name: String::from(name),
        })?;

        self.cache_type_oid.insert(name.to_string().into(), oid);
        Ok(oid)
    }

    pub(crate) async fn get_nullable_for_columns(
        &mut self,
        stmt_id: Oid,
        meta: &PgStatementMetadata,
    ) -> Result<Vec<Option<bool>>, Error> {
        if meta.columns.is_empty() {
            return Ok(vec![]);
        }

        let mut nullable_query = String::from("SELECT NOT pg_attribute.attnotnull FROM (VALUES ");
        let mut args = PgArguments::default();

        for (i, (column, bind)) in meta.columns.iter().zip((1..).step_by(3)).enumerate() {
            if !args.buffer.is_empty() {
                nullable_query += ", ";
            }

            let _ = write!(
                nullable_query,
                "(${}::int4, ${}::int4, ${}::int2)",
                bind,
                bind + 1,
                bind + 2
            );

            args.add(i as i32);
            args.add(column.relation_id);
            args.add(column.relation_attribute_no);
        }

        nullable_query.push_str(
            ") as col(idx, table_id, col_idx) \
            LEFT JOIN pg_catalog.pg_attribute \
                ON table_id IS NOT NULL \
               AND attrelid = table_id \
               AND attnum = col_idx \
            ORDER BY col.idx",
        );

        let mut nullables = query_scalar_with::<_, Option<bool>, _>(&nullable_query, args)
            .fetch_all(&mut *self)
            .await?;

        // if it's cockroachdb skip this step #1248
        if !self.stream.parameter_statuses.contains_key("crdb_version") {
            // patch up our null inference with data from EXPLAIN
            let nullable_patch = self
                .nullables_from_explain(stmt_id, meta.parameters.len())
                .await?;

            for (nullable, patch) in nullables.iter_mut().zip(nullable_patch) {
                *nullable = patch.or(*nullable);
            }
        }

        Ok(nullables)
    }

    /// Infer nullability for columns of this statement using EXPLAIN VERBOSE.
    ///
    /// This currently only marks columns that are on the inner half of an outer join
    /// and returns `None` for all others.
    async fn nullables_from_explain(
        &mut self,
        stmt_id: Oid,
        params_len: usize,
    ) -> Result<Vec<Option<bool>>, Error> {
        let mut explain = format!(
            "EXPLAIN (VERBOSE, FORMAT JSON) EXECUTE sqlx_s_{}",
            stmt_id.0
        );
        let mut comma = false;

        if params_len > 0 {
            explain += "(";

            // fill the arguments list with NULL, which should theoretically be valid
            for _ in 0..params_len {
                if comma {
                    explain += ", ";
                }

                explain += "NULL";
                comma = true;
            }

            explain += ")";
        }

        let (Json([explain]),): (Json<[Explain; 1]>,) = query_as(&explain).fetch_one(self).await?;

        let mut nullables = Vec::new();

        if let Some(outputs) = &explain.plan.output {
            nullables.resize(outputs.len(), None);
            visit_plan(&explain.plan, outputs, &mut nullables);
        }

        Ok(nullables)
    }
}

fn visit_plan(plan: &Plan, outputs: &[String], nullables: &mut Vec<Option<bool>>) {
    if let Some(plan_outputs) = &plan.output {
        // all outputs of a Full Join must be marked nullable
        // otherwise, all outputs of the inner half of an outer join must be marked nullable
        if let Some("Full") | Some("Inner") = plan
            .join_type
            .as_deref()
            .or(plan.parent_relation.as_deref())
        {
            for output in plan_outputs {
                if let Some(i) = outputs.iter().position(|o| o == output) {
                    // N.B. this may produce false positives but those don't cause runtime errors
                    nullables[i] = Some(true);
                }
            }
        }
    }

    if let Some(plans) = &plan.plans {
        if let Some("Left") | Some("Right") = plan.join_type.as_deref() {
            for plan in plans {
                visit_plan(plan, outputs, nullables);
            }
        }
    }
}

#[derive(serde::Deserialize)]
struct Explain {
    #[serde(rename = "Plan")]
    plan: Plan,
}

#[derive(serde::Deserialize)]
struct Plan {
    #[serde(rename = "Join Type")]
    join_type: Option<String>,
    #[serde(rename = "Parent Relationship")]
    parent_relation: Option<String>,
    #[serde(rename = "Output")]
    output: Option<Vec<String>>,
    #[serde(rename = "Plans")]
    plans: Option<Vec<Plan>>,
}
