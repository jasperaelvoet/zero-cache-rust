//! Port of `zero-cache/src/types/sql.ts`.
//!
//! Postgres identifier and string-literal escaping.

/// Escapes an identifier with double quotes, doubling any embedded quotes.
/// Port of `id`.
pub fn id(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Escapes and comma-separates a list of identifiers. Port of `idList`.
pub fn id_list<'a, I: IntoIterator<Item = &'a str>>(names: I) -> String {
    names.into_iter().map(id).collect::<Vec<_>>().join(",")
}

/// Escapes a string literal with single quotes, doubling any embedded quotes.
/// Port of `lit`.
pub fn lit(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_escaping() {
        assert_eq!(id("simple"), "\"simple\"");
        assert_eq!(id("containing\"quotes"), "\"containing\"\"quotes\"");
        assert_eq!(id("name.with.dots"), "\"name.with.dots\"");
    }

    #[test]
    fn id_list_escaping() {
        assert_eq!(
            id_list(["simple", "containing\"quotes", "name.with.dots"]),
            "\"simple\",\"containing\"\"quotes\",\"name.with.dots\""
        );
        assert_eq!(id_list(["singleton"]), "\"singleton\"");
    }

    #[test]
    fn lit_escaping() {
        assert_eq!(lit("simple"), "'simple'");
        assert_eq!(lit("containing'quotes"), "'containing''quotes'");
        assert_eq!(lit("multiple'quotes'here"), "'multiple''quotes''here'");
        assert_eq!(lit(""), "''");
    }
}
