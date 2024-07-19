use sqlx_core::database::Database;

#[derive(PartialEq, Eq)]
#[allow(dead_code)]
pub enum ParamChecking {
    Strong,
    Weak,
}

/// Database extension trait
///
/// This extension trait is primarily intended at providing helpers to generate
/// error messages.
pub trait DatabaseExt: Database {
    /// Stringified path to the database type.
    ///
    /// Examples:
    /// - `"sqlx::postgres::Postgres"`
    /// - `"sqlx::sqlite::Sqlite"`
    const DATABASE_PATH: &'static str;
    /// Stringified path to the row type.
    ///
    /// Examples:
    /// - `"sqlx::postgres::PgRow"`
    /// - `"sqlx::sqlite::SqliteRow"`
    const ROW_PATH: &'static str;
    /// Display name for the database
    ///
    /// Examples:
    /// - `"PostgreSQL"`
    /// - `"SQLite"`
    const NAME: &'static str;

    const PARAM_CHECKING: ParamChecking;

    /// Get `Self::DATABASE_PATH` as a parsed `syn::Path`
    fn db_path() -> syn::Path {
        syn::parse_str(Self::DATABASE_PATH).unwrap()
    }

    /// Get `Self::ROW_PATH` as a parsed `syn::Path`
    fn row_path() -> syn::Path {
        syn::parse_str(Self::ROW_PATH).unwrap()
    }

    /// Get the stringified Rust type for DB input parameters with the provided type info
    ///
    /// Examples:
    /// - `sqlx::postgres::Postgres::param_type_for_id(&LazyPgTypeInfo::INT2)` returns `Some("i16")`
    /// - `sqlx::sqlite::Sqlite::param_type_for_id(&SqliteTypeInfo(DataType::Blob))` returns `Some("Vec<u8>")`
    /// - `sqlx::sqlite::Sqlite::param_type_for_id(&SqliteTypeInfo(DataType::DateTime))` returns `Some("sqlx::types::chrono::DateTime<_>")`
    fn param_type_for_id(id: &Self::TypeInfo) -> Option<&'static str>;

    /// Get the stringified Rust type for DB output results with the provided type info
    ///
    /// Examples:
    /// - `sqlx::postgres::Postgres::return_type_for_id(&LazyPgTypeInfo::INT2)` returns `Some("i16")`
    /// - `sqlx::sqlite::Sqlite::return_type_for_id(&SqliteTypeInfo(DataType::Blob))` returns `Some("Vec<u8>")`
    /// - `sqlx::sqlite::Sqlite::return_type_for_id(&SqliteTypeInfo(DataType::DateTime))` returns `Some("sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>")`
    fn return_type_for_id(id: &Self::TypeInfo) -> Option<&'static str>;

    /// Get the name of the `sqlx` feature (if any) to enable support for the provided type.
    ///
    /// Example:
    /// - `sqlx::postgres::Postgres::return_type_for_id(&LazyPgTypeInfo::UUID)` returns `Some("uuid")`
    fn get_feature_gate(info: &Self::TypeInfo) -> Option<&'static str>;
}

macro_rules! impl_database_ext {
    (
        $database:path {
            $($(#[$meta:meta])? $ty:ty $(| $input:ty)?),*$(,)?
        },
        ParamChecking::$param_checking:ident,
        feature-types: $ty_info:ident => $get_gate:expr,
        row = $row:path,
        name = $db_name:literal
    ) => {
        impl $crate::database::DatabaseExt for $database {
            const DATABASE_PATH: &'static str = stringify!($database);
            const ROW_PATH: &'static str = stringify!($row);
            const PARAM_CHECKING: $crate::database::ParamChecking = $crate::database::ParamChecking::$param_checking;
            const NAME: &'static str = $db_name;

            fn param_type_for_id(info: &Self::TypeInfo) -> Option<&'static str> {
                match () {
                    // $(
                    //     $(#[$meta])?
                    //     _ if <$ty as sqlx_core::types::Type<$database>>::type_info() == *info => Some(input_ty!($ty $(, $input)?)),
                    // )*
                    $(
                        $(#[$meta])?
                        _ if <$ty as sqlx_core::types::Type<$database>>::compatible(info) => Some(input_ty!($ty $(, $input)?)),
                    )*
                    _ => None
                }
            }

            fn return_type_for_id(info: &Self::TypeInfo) -> Option<&'static str> {
                match () {
                    // $(
                    //     $(#[$meta])?
                    //     _ if <$ty as sqlx_core::types::Type<$database>>::type_info() == *info => return Some(stringify!($ty)),
                    // )*
                    $(
                        $(#[$meta])?
                        _ if <$ty as sqlx_core::types::Type<$database>>::compatible(info) => return Some(stringify!($ty)),
                    )*
                    _ => None
                }
            }

            fn get_feature_gate($ty_info: &Self::TypeInfo) -> Option<&'static str> {
                $get_gate
            }
        }
    }
}

macro_rules! input_ty {
    ($ty:ty, $input:ty) => {
        stringify!($input)
    };
    ($ty:ty) => {
        stringify!($ty)
    };
}

#[cfg(feature = "postgres")]
mod postgres;

#[cfg(feature = "mysql")]
mod mysql;

#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "mssql")]
mod mssql;
