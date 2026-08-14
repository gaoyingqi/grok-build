//! 进程内 Send 幂等映射。
//!
//! 此模块只冻结 M0 的 L1 幂等边界：同一 `(scope_id, session_id, submission_id)`
//! 与稳定指纹命中时返回原 turn；指纹不同则 fail-closed。

use std::collections::BTreeMap;

use crate::MentionId;

/// SubmissionMap 的复合键。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SubmissionKey {
    scope_id: String,
    session_id: String,
    submission_id: String,
}

/// 首次受理后需要稳定保留的记录。
#[derive(Debug, Clone, PartialEq, Eq)]
struct SubmissionRecord {
    fingerprint: String,
    turn_id: String,
}

/// Send 的进程内幂等映射。
#[derive(Debug, Default)]
pub(crate) struct SubmissionMap {
    entries: BTreeMap<SubmissionKey, SubmissionRecord>,
}

/// 一次提交登记的确定性结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubmissionDecision {
    /// 首次看到该 key 和指纹。
    Accepted { turn_id: String },
    /// 该 key 命中同一稳定指纹。
    Duplicate { turn_id: String },
    /// 该 key 已存在但稳定指纹不同。
    FingerprintConflict,
}

impl SubmissionMap {
    /// 删除尚未实际写入 ACP 的首次提交记录。
    ///
    /// dispatch 在 sidecar 启动、自动 load 或 stdin 写入失败时调用本方法，避免后续
    /// 重试被误判为已投递的 duplicate；已成功写入 prompt 的记录绝不能移除。
    pub(crate) fn forget(&mut self, scope_id: &str, session_id: &str, submission_id: &str) {
        self.entries.remove(&SubmissionKey {
            scope_id: scope_id.to_string(),
            session_id: session_id.to_string(),
            submission_id: submission_id.to_string(),
        });
    }

    /// 以稳定、长度前缀的 canonical 输入登记 Send，避免字段拼接歧义。
    pub(crate) fn record(
        &mut self,
        scope_id: &str,
        session_id: &str,
        submission_id: &str,
        text: &str,
        mentions: &[MentionId],
    ) -> SubmissionDecision {
        let key = SubmissionKey {
            scope_id: scope_id.to_string(),
            session_id: session_id.to_string(),
            submission_id: submission_id.to_string(),
        };
        let fingerprint = canonicalize(scope_id, session_id, text, mentions);

        if let Some(existing) = self.entries.get(&key) {
            return if existing.fingerprint == fingerprint {
                SubmissionDecision::Duplicate {
                    turn_id: existing.turn_id.clone(),
                }
            } else {
                SubmissionDecision::FingerprintConflict
            };
        }

        // M0 的 turn_id 按协议恒等于 submission_id，不生成替代标识。
        let turn_id = submission_id.to_string();
        self.entries.insert(
            key,
            SubmissionRecord {
                fingerprint,
                turn_id: turn_id.clone(),
            },
        );
        SubmissionDecision::Accepted { turn_id }
    }
}

/// 构造稳定指纹输入；mention 先按 `(kind, id)` 字典序排序，保留重复项。
fn canonicalize(scope_id: &str, session_id: &str, text: &str, mentions: &[MentionId]) -> String {
    let mut sorted_mentions = mentions.to_vec();
    sorted_mentions.sort();

    let mut output = String::new();
    append_part(&mut output, scope_id);
    append_part(&mut output, session_id);
    append_part(&mut output, text);
    output.push_str(&sorted_mentions.len().to_string());
    output.push(':');
    for mention in sorted_mentions {
        append_part(&mut output, &mention.kind);
        append_part(&mut output, &mention.id);
    }
    output
}

/// 向 canonical 输入追加长度前缀字符串，避免字段边界碰撞。
fn append_part(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
}
