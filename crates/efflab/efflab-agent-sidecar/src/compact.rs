//! sidecar 精简上下文压缩：按固定 200k 窗口估算，超阈值后对更早回合做一次摘要。
//!
//! 不提供用户可配置的上下文长度；失败时跳过压缩、不改写已有 journal。

use serde_json::Value;

use crate::session_store::SessionRecord;

/// 产品默认模型上下文窗口（token）；不向用户开放设置。
pub const CONTEXT_WINDOW_TOKENS: u64 = 200_000;
/// 触发压缩的窗口占用百分比，给当前回合的模型输出预留余量。
pub const COMPACT_THRESHOLD_PERCENT: u64 = 70;
/// 压缩后仍按原文保留的最近用户回合数（含当前 prompt）。
pub const COMPACT_KEEP_USER_TURNS: usize = 2;
/// 摘要正文上限，避免 compact 记录撑满 journal 行。
const COMPACT_SUMMARY_MAX_CHARS: usize = 32_768;

/// 摘要模型使用的固定说明；不要求调用工具。
pub const COMPACT_REQUEST_PROMPT: &str = "\
Summarize the conversation so far for a successor assistant. \
Preserve the user's requests, decisions, named entities, and remaining work. \
Do not call tools. Reply with the summary only.";

/// 压缩后插入模型上下文的助手确认，避免连续两条 user 消息。
pub const COMPACT_ACK: &str = "Understood. I will continue from this summary.";

/// 生产压缩阈值。窗口固定 200k，不向用户开放，也不读环境变量。
///
/// debug 测试若要压低阈值，必须走 `TestSeam` 文件；sidecar 启动会 `sanitize_env`。
pub fn compact_threshold_tokens() -> u64 {
    CONTEXT_WINDOW_TOKENS.saturating_mul(COMPACT_THRESHOLD_PERCENT) / 100
}

/// 粗估 token：按字符数 / 2，偏保守以便中文更早触发压缩。
pub fn estimate_text_tokens(text: &str) -> u64 {
    u64::try_from(text.chars().count().div_ceil(2)).unwrap_or(u64::MAX)
}

/// 粗估一组 Chat Completions 消息的 token。
pub fn estimate_messages_tokens(messages: &[Value]) -> u64 {
    messages.iter().fold(0_u64, |acc, message| {
        acc.saturating_add(estimate_text_tokens(&message.to_string()))
    })
}

/// 返回最新一条 compact_summary 的正文与覆盖截止 sequence。
pub fn latest_compact(records: &[SessionRecord]) -> Option<(&str, u64)> {
    records.iter().rev().find_map(|record| match record {
        SessionRecord::CompactSummary {
            text,
            covered_until_sequence,
            ..
        } => Some((text.as_str(), *covered_until_sequence)),
        _ => None,
    })
}

/// 计算本轮应摘要的前缀（含截止下标与 covered_until sequence）。
///
/// 只压缩最新 compact 之后、且位于最近 [`COMPACT_KEEP_USER_TURNS`] 个 user 之前的记录。
pub fn compact_prefix_end(records: &[SessionRecord]) -> Option<(usize, u64)> {
    let start = match latest_compact(records) {
        Some((_, covered)) => records
            .iter()
            .position(|record| record.sequence() > covered)
            .unwrap_or(records.len()),
        None => 0,
    };
    let user_positions = records
        .iter()
        .enumerate()
        .skip(start)
        .filter(|(_, record)| matches!(record, SessionRecord::User { .. }))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if user_positions.len() <= COMPACT_KEEP_USER_TURNS {
        return None;
    }
    let tail_first_user = user_positions[user_positions.len() - COMPACT_KEEP_USER_TURNS];
    if tail_first_user == 0 {
        return None;
    }
    let prefix_end = tail_first_user.checked_sub(1)?;
    Some((prefix_end, records[prefix_end].sequence()))
}

/// 把摘要截到 journal 行可接受的字符上限。
pub fn truncate_summary(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= COMPACT_SUMMARY_MAX_CHARS {
        return trimmed.to_owned();
    }
    trimmed.chars().take(COMPACT_SUMMARY_MAX_CHARS).collect()
}

/// 构造插入 transcript 头部的摘要消息对。
pub fn summary_messages(summary: &str) -> [Value; 2] {
    [
        serde_json::json!({
            "role": "user",
            "content": format!(
                "<conversation_summary>\n{summary}\n</conversation_summary>"
            )
        }),
        serde_json::json!({
            "role": "assistant",
            "content": COMPACT_ACK
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        COMPACT_KEEP_USER_TURNS, CONTEXT_WINDOW_TOKENS, compact_prefix_end, compact_threshold_tokens,
        estimate_text_tokens, truncate_summary,
    };
    use crate::session_store::SessionRecord;

    fn user(sequence: u64, prompt: &str, text: &str) -> SessionRecord {
        SessionRecord::user(sequence, prompt, text)
    }

    fn terminal(sequence: u64, prompt: &str) -> SessionRecord {
        SessionRecord::turn_terminal(sequence, prompt, "completed")
    }

    #[test]
    fn production_threshold_is_seventy_percent_of_200k() {
        assert_eq!(CONTEXT_WINDOW_TOKENS, 200_000);
        assert_eq!(compact_threshold_tokens(), 140_000);
        assert_eq!(COMPACT_KEEP_USER_TURNS, 2);
    }

    #[test]
    fn estimate_is_conservative_for_cjk() {
        assert_eq!(estimate_text_tokens("abcd"), 2);
        assert_eq!(estimate_text_tokens("中文摘要"), 2);
    }

    #[test]
    fn prefix_requires_more_user_turns_than_keep() {
        let records = vec![
            user(0, "p1", "one"),
            terminal(1, "p1"),
            user(2, "p2", "two"),
            terminal(3, "p2"),
        ];
        assert_eq!(compact_prefix_end(&records), None);

        let records = vec![
            user(0, "p1", "one"),
            terminal(1, "p1"),
            user(2, "p2", "two"),
            terminal(3, "p2"),
            user(4, "p3", "three"),
        ];
        assert_eq!(compact_prefix_end(&records), Some((1, 1)));
    }

    #[test]
    fn prefix_starts_after_existing_compact_summary() {
        let records = vec![
            user(0, "p1", "one"),
            terminal(1, "p1"),
            SessionRecord::compact_summary(2, "p2", 1, "old summary"),
            user(3, "p2", "two"),
            terminal(4, "p2"),
            user(5, "p3", "three"),
        ];
        assert_eq!(compact_prefix_end(&records), None);

        let mut records = records;
        records.push(terminal(6, "p3"));
        records.push(user(7, "p4", "four"));
        assert_eq!(compact_prefix_end(&records), Some((4, 4)));
    }

    #[test]
    fn truncate_summary_keeps_short_text() {
        assert_eq!(truncate_summary("  hello  "), "hello");
    }
}
