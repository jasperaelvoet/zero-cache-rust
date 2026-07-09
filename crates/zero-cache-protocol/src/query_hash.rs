//! Port of `zero-protocol/src/query-hash.ts`: `hashOfAST`/`hashOfNameAndArgs`
//! — the content-addressed IDs used to identify custom/ad-hoc queries
//! (`CustomQueryRecord.id`, `InternalQueryRecord.id`) across the CVR and the
//! `transform-query.ts` cache-key computation
//! (`view-syncer::transform_query_cache_key`).
//!
//! `hashOfAST` normalizes the AST (matching upstream), `JSON.stringify`s it,
//! and hashes with `h64`, base36-encoded. Upstream also memoizes the result
//! in a `WeakMap<AST, string>` keyed by object identity — that's a pure
//! micro-optimization for repeated calls with the exact same AST object, not
//! observable behavior; skipped here since Rust has no ambient WeakMap and
//! this port's convention is to keep such caches at the call site if ever
//! needed, not bake them into the pure function.

use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_shared::hash::h64;

use crate::ast::{normalize_ast, Ast};
use crate::ast_json::ast_to_json;

/// Port of `hashOfAST`.
pub fn hash_of_ast(ast: &Ast) -> String {
    let normalized = normalize_ast(ast);
    let json = ast_to_json(&normalized).stringify();
    to_base36(h64(&json))
}

/// Port of `hashOfNameAndArgs`. `args` are passed as an already-serialized
/// [`JsonValue`] array (the caller's `args: Vec<JsonValue>`) rather than a
/// generic `unknown[]`, since this port has no untyped-value type broader
/// than `JsonValue`.
pub fn hash_of_name_and_args(name: &str, args: &[JsonValue]) -> String {
    let args_string = JsonValue::Array(args.to_vec()).stringify();
    to_base36(h64(&format!("{name}:{args_string}")))
}

/// Unpadded base36 encoding, matching JavaScript's `n.toString(36)`. `pub`
/// since other modules with their own `randInt(...).toString(36)`-style
/// ID generation (e.g. `view_syncer_lifecycle::random_id`) need the exact
/// same encoding — not worth a second copy.
pub fn to_base36(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ColumnReference, Condition, LiteralValue, SimpleOperator, ValuePosition};

    #[test]
    fn to_base36_matches_js_to_string_36() {
        assert_eq!(to_base36(0), "0");
        assert_eq!(to_base36(35), "z");
        assert_eq!(to_base36(36), "10");
        assert_eq!(to_base36(u64::MAX), "3w5e11264sgsf");
    }

    /// Golden regression-lock on the exact hash outputs. Query hashes are
    /// content-addressed IDs persisted in the CVR and shared across the
    /// client/server boundary, so the whole `normalize_ast` → `ast_to_json`
    /// → `stringify` → `h64` → base36 chain must stay byte-stable: any drift
    /// would silently invalidate existing query IDs. These pin the current
    /// output so such a change fails loudly here.
    #[test]
    fn hash_outputs_are_byte_stable() {
        assert_eq!(hash_of_ast(&Ast::table("issues")), "3fscdztul0ozz");
        assert_eq!(
            hash_of_name_and_args("q", &[JsonValue::Number(1.0)]),
            "12snfr0sk2pt3"
        );
    }

    #[test]
    fn hash_of_ast_is_stable_for_identical_asts() {
        let ast = Ast::table("issues");
        assert_eq!(hash_of_ast(&ast), hash_of_ast(&ast));
    }

    #[test]
    fn hash_of_ast_ignores_where_clause_ordering_via_normalization() {
        let cond = |col: &str| Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference { name: col.into() }),
            right: ValuePosition::Literal(LiteralValue::Number(1.0)),
        };
        let a = Ast {
            table: "t".into(),
            where_: Some(Condition::And {
                conditions: vec![cond("a"), cond("b")],
            }),
            ..Default::default()
        };
        let b = Ast {
            table: "t".into(),
            where_: Some(Condition::And {
                conditions: vec![cond("b"), cond("a")],
            }),
            ..Default::default()
        };
        assert_eq!(
            hash_of_ast(&a),
            hash_of_ast(&b),
            "normalization should make ordering-only differences hash equal"
        );
    }

    #[test]
    fn hash_of_ast_differs_for_different_tables() {
        assert_ne!(
            hash_of_ast(&Ast::table("issues")),
            hash_of_ast(&Ast::table("comments"))
        );
    }

    #[test]
    fn hash_of_name_and_args_is_stable_and_sensitive_to_args() {
        let args1 = vec![JsonValue::Number(1.0)];
        let args2 = vec![JsonValue::Number(2.0)];
        assert_eq!(
            hash_of_name_and_args("q", &args1),
            hash_of_name_and_args("q", &args1)
        );
        assert_ne!(
            hash_of_name_and_args("q", &args1),
            hash_of_name_and_args("q", &args2)
        );
        assert_ne!(
            hash_of_name_and_args("q", &args1),
            hash_of_name_and_args("other", &args1)
        );
    }
}
