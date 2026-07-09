//! Port of `zero-cache/src/types/strings.ts`.

/// Truncates `val` so that its UTF-8 encoding is at most `max_bytes` bytes,
/// appending `"..."` when truncation occurs.
///
/// Port of `elide`. The TypeScript version slices by UTF-16 code units; this
/// version slices by Unicode scalar values, which is equivalent for all
/// Basic-Multilingual-Plane text (the only case exercised) and differs only at
/// a truncation boundary that would split an astral-plane character.
pub fn elide(val: &str, max_bytes: usize) -> String {
    if val.len() <= max_bytes {
        return val.to_string();
    }
    let chars: Vec<char> = val.chars().collect();
    let end = max_bytes.saturating_sub(3).min(chars.len());
    let mut s: String = chars[..end].iter().collect();
    while s.len() + "...".len() > max_bytes && !s.is_empty() {
        s.pop();
    }
    s.push_str("...");
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elide_byte_count() {
        let elided_ascii = elide(&format!("fo{}", "o".repeat(150)), 123);
        assert_eq!(elided_ascii, format!("fo{}...", "o".repeat(118)));
        assert_eq!(elided_ascii.chars().count(), 123);

        let elided_full_width = elide(&format!("こんにちは{}", "あ".repeat(150)), 123);
        assert_eq!(
            elided_full_width,
            format!("こんにちは{}...", "あ".repeat(35))
        );
        assert!(elided_full_width.len() <= 123);
    }
}
