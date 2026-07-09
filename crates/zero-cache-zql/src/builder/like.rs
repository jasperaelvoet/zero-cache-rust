//! Port of `zql/src/builder/like.ts`.
//!
//! Compiles a SQL `LIKE`/`ILIKE` pattern into a matcher: `%` -> any run of
//! characters, `_` -> any single character, `\` escapes the next
//! character, anything else matches literally. Patterns with no wildcards
//! skip regex entirely (plain string, or lowercased string, comparison).

use regex::Regex;

/// A pattern compiled from `LIKE`/`ILIKE`'s right-hand side, ready to test
/// against a `lhs` string. Port of `getLikePredicate`'s returned closure
/// type (`SimplePredicateNoNull`, specialized to strings).
pub enum LikePredicate {
    Exact(String),
    ExactCaseInsensitive(String),
    Regex(Regex),
}

impl LikePredicate {
    pub fn matches(&self, lhs: &str) -> bool {
        match self {
            LikePredicate::Exact(pattern) => lhs == pattern,
            LikePredicate::ExactCaseInsensitive(pattern) => lhs.to_lowercase() == *pattern,
            LikePredicate::Regex(re) => re.is_match(lhs),
        }
    }
}

/// Whether `pattern` contains any of `_`, `%`, or `\` — if not, it's a
/// plain string comparison, no regex needed. Port of `likePatternRe`.
fn has_wildcards(pattern: &str) -> bool {
    pattern.contains(['_', '%', '\\'])
}

/// Port of `getLikePredicate`/`getLikeOp`. `case_insensitive` corresponds
/// to upstream's `flags === 'i'` (i.e. `ILIKE` vs `LIKE`).
pub fn get_like_predicate(pattern: &str, case_insensitive: bool) -> LikePredicate {
    if !has_wildcards(pattern) {
        return if case_insensitive {
            LikePredicate::ExactCaseInsensitive(pattern.to_lowercase())
        } else {
            LikePredicate::Exact(pattern.to_string())
        };
    }
    LikePredicate::Regex(pattern_to_regex(pattern, case_insensitive))
}

/// Port of `patternToRegExp`. Uses `(?s)` (dotall, matching upstream's `s`
/// flag choice — see its comment on why `m` was wrong) so `.`/`.*` match
/// newlines too, and anchors the whole string with `^`/`$`.
fn pattern_to_regex(source: &str, case_insensitive: bool) -> Regex {
    let mut pattern = String::from("^");
    let chars: Vec<char> = source.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let mut c = chars[i];
        match c {
            '%' => pattern.push_str(".*"),
            '_' => pattern.push('.'),
            '\\' => {
                if i == chars.len() - 1 {
                    panic!("LIKE pattern must not end with escape character");
                }
                i += 1;
                c = chars[i];
                if is_special_regex_char(c) {
                    pattern.push('\\');
                }
                pattern.push(c);
            }
            _ => {
                if is_special_regex_char(c) {
                    pattern.push('\\');
                }
                pattern.push(c);
            }
        }
        i += 1;
    }
    pattern.push('$');
    let flags = if case_insensitive { "(?is)" } else { "(?s)" };
    Regex::new(&format!("{flags}{pattern}")).expect("LIKE pattern compiles to a valid regex")
}

fn is_special_regex_char(c: char) -> bool {
    matches!(
        c,
        '$' | '(' | ')' | '*' | '+' | '.' | '?' | '[' | ']' | '\\' | '^' | '{' | '|' | '}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_wildcards_is_exact_match() {
        let p = get_like_predicate("hello", false);
        assert!(p.matches("hello"));
        assert!(!p.matches("hello world"));
    }

    #[test]
    fn no_wildcards_case_insensitive() {
        let p = get_like_predicate("Hello", true);
        assert!(p.matches("hello"));
        assert!(p.matches("HELLO"));
        assert!(!p.matches("hellox"));
    }

    #[test]
    fn percent_matches_any_run() {
        let p = get_like_predicate("foo%", false);
        assert!(p.matches("foo"));
        assert!(p.matches("foobar"));
        assert!(!p.matches("xfoo"));
    }

    #[test]
    fn underscore_matches_single_char() {
        let p = get_like_predicate("f_o", false);
        assert!(p.matches("foo"));
        assert!(!p.matches("fo"));
        assert!(!p.matches("fooo"));
    }

    #[test]
    fn escaped_wildcard_is_literal() {
        let p = get_like_predicate("100\\%", false);
        assert!(p.matches("100%"));
        assert!(!p.matches("100x"));
    }

    #[test]
    fn special_regex_chars_are_escaped() {
        let p = get_like_predicate("a.b", false);
        assert!(p.matches("a.b"));
        assert!(!p.matches("axb"));
    }

    #[test]
    fn newline_matched_by_dotall_wildcards() {
        let p = get_like_predicate("a%b", false);
        assert!(p.matches("a\nb"));
        // "_" maps to "." which, under the dotall flag, also matches a single
        // newline character (matching upstream's choice of the `s` flag).
        let p2 = get_like_predicate("a_b", false);
        assert!(p2.matches("a\nb"));
    }

    #[test]
    #[should_panic(expected = "must not end with escape character")]
    fn trailing_escape_panics() {
        get_like_predicate("abc\\", false);
    }
}
