//! Port of `zero-cache/src/types/names.ts`.

/// A Postgres table reference (schema + name).
pub struct TableName<'a> {
    pub schema: &'a str,
    pub name: &'a str,
}

/// Returns the SQLite ("lite") table name for a Postgres table: the bare name
/// for the `public` schema, otherwise `schema.name`. Port of `liteTableName`.
pub fn lite_table_name(t: &TableName<'_>) -> String {
    if t.schema == "public" {
        t.name.to_string()
    } else {
        format!("{}.{}", t.schema, t.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_schema() {
        assert_eq!(
            lite_table_name(&TableName {
                schema: "public",
                name: "issues"
            }),
            "issues"
        );
    }

    #[test]
    fn zero_schema() {
        assert_eq!(
            lite_table_name(&TableName {
                schema: "zero",
                name: "clients"
            }),
            "zero.clients"
        );
    }
}
