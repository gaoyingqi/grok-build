//! 进程内 Send 幂等映射。
//!
//! 此模块只冻结 M0 的 L1 幂等边界：同一 `(scope_id, session_id, submission_id)`
//! 与稳定指纹命中时返回原 turn；指纹不同则 fail-closed。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::MentionId;

/// Send 在 actor 与调用方之间共享的写入结局。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendTicketState {
    /// command 尚未取得 prompt 写入所有权。
    Waiting,
    /// actor 已取得所有权，但尚未知道写入是否完成。
    Claimed,
    /// 完整 prompt wire 已成功写入并刷新。
    Written,
    /// 可以证明没有任何 prompt wire 写入。
    NotWritten,
    /// 写入过程遇到无法判断是否已提交的错误，必须保留幂等记录。
    MayHaveBeenWritten,
    /// 调用方在 actor 取得所有权前已放弃 command。
    Abandoned,
}

/// Send 调用方与 actor 之间的写入所有权票据。
///
/// 票据同时存放在 SubmissionMap 记录中，因此调用方超时后 actor 仍能发布最终写入
/// 结局；后续相同 submission 的重试只在明确 `NotWritten` 时重新受理。
#[derive(Debug, Clone)]
pub(crate) struct SendTicket {
    state: Arc<AtomicU8>,
}

impl SendTicket {
    const WAITING: u8 = 0;
    const CLAIMED: u8 = 1;
    const WRITTEN: u8 = 2;
    const NOT_WRITTEN: u8 = 3;
    const MAY_HAVE_BEEN_WRITTEN: u8 = 4;
    const ABANDONED: u8 = 5;

    /// 新建仍可由调用方撤销的 Send 票据。
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(Self::WAITING)),
        }
    }

    /// 读取当前结局；未知内部值按不确定写入处理，保持 fail-closed。
    pub(crate) fn state(&self) -> SendTicketState {
        match self.state.load(Ordering::Acquire) {
            Self::WAITING => SendTicketState::Waiting,
            Self::CLAIMED => SendTicketState::Claimed,
            Self::WRITTEN => SendTicketState::Written,
            Self::NOT_WRITTEN => SendTicketState::NotWritten,
            Self::MAY_HAVE_BEEN_WRITTEN => SendTicketState::MayHaveBeenWritten,
            Self::ABANDONED => SendTicketState::Abandoned,
            _ => SendTicketState::MayHaveBeenWritten,
        }
    }

    /// actor 在实际写 prompt 前独占所有权；已撤销的调用绝不能再写入。
    pub(crate) fn claim_for_prompt(&self) -> bool {
        self.state
            .compare_exchange(
                Self::WAITING,
                Self::CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// 调用方超时时尝试撤销；返回 false 表示 actor 已可能写入 prompt。
    pub(crate) fn abandon(&self) -> bool {
        self.state
            .compare_exchange(
                Self::WAITING,
                Self::ABANDONED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// 标记完整 prompt wire 已写入；其它终态不允许被覆盖。
    pub(crate) fn mark_written(&self) -> bool {
        self.state
            .compare_exchange(
                Self::CLAIMED,
                Self::WRITTEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// 标记可以证明没有写入 prompt；调用方放弃状态保持不变。
    pub(crate) fn mark_not_written(&self) {
        loop {
            match self.state.load(Ordering::Acquire) {
                Self::WAITING | Self::CLAIMED => {
                    let current = self.state.load(Ordering::Acquire);
                    if self
                        .state
                        .compare_exchange(
                            current,
                            Self::NOT_WRITTEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                Self::ABANDONED
                | Self::NOT_WRITTEN
                | Self::WRITTEN
                | Self::MAY_HAVE_BEEN_WRITTEN
                | _ => return,
            }
        }
    }

    /// 标记写入过程中出现不确定错误；该结局永不允许自动重试 prompt。
    pub(crate) fn mark_may_have_been_written(&self) -> bool {
        self.state
            .compare_exchange(
                Self::CLAIMED,
                Self::MAY_HAVE_BEEN_WRITTEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// 判断调用方是否已经赢得撤销竞争。
    pub(crate) fn is_abandoned(&self) -> bool {
        self.state() == SendTicketState::Abandoned
    }

    /// 判断旧 submission 是否已经可以安全地重新登记。
    fn can_retry_without_prompt(&self) -> bool {
        matches!(
            self.state(),
            SendTicketState::NotWritten | SendTicketState::Abandoned
        )
    }
}

/// SubmissionMap 的复合键。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SubmissionKey {
    scope_id: String,
    session_id: String,
    submission_id: String,
}

/// 首次受理后需要稳定保留的记录。
#[derive(Debug, Clone)]
struct SubmissionRecord {
    fingerprint: String,
    turn_id: String,
    ticket: SendTicket,
}

/// Send 的进程内幂等映射。
#[derive(Debug, Default)]
pub(crate) struct SubmissionMap {
    entries: BTreeMap<SubmissionKey, SubmissionRecord>,
}

/// 一次提交登记的确定性结果。
#[derive(Debug, Clone)]
pub(crate) enum SubmissionDecision {
    /// 首次看到该 key 和指纹；ticket 会随 actor 生命周期共享。
    Accepted { turn_id: String, ticket: SendTicket },
    /// 该 key 命中同一稳定指纹。
    Duplicate { turn_id: String },
    /// 该 key 已存在但稳定指纹不同。
    FingerprintConflict,
}

impl SubmissionMap {
    /// 仅删除与指定 ticket 相同的尚未写入记录，避免旧调用回滚新一代 submission。
    pub(crate) fn forget(
        &mut self,
        scope_id: &str,
        session_id: &str,
        submission_id: &str,
        ticket: &SendTicket,
    ) {
        let key = SubmissionKey {
            scope_id: scope_id.to_string(),
            session_id: session_id.to_string(),
            submission_id: submission_id.to_string(),
        };
        if self
            .entries
            .get(&key)
            .is_some_and(|record| Arc::ptr_eq(&record.ticket.state, &ticket.state))
        {
            self.entries.remove(&key);
        }
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
            if existing.ticket.can_retry_without_prompt() {
                // 只有 actor 已证明没有写 prompt 时才清掉旧记录，避免不确定写入制造第二次 prompt。
                self.entries.remove(&key);
            } else {
                return if existing.fingerprint == fingerprint {
                    SubmissionDecision::Duplicate {
                        turn_id: existing.turn_id.clone(),
                    }
                } else {
                    SubmissionDecision::FingerprintConflict
                };
            }
        }

        // M0 的 turn_id 按协议恒等于 submission_id，不生成替代标识。
        let turn_id = submission_id.to_string();
        let ticket = SendTicket::new();
        self.entries.insert(
            key,
            SubmissionRecord {
                fingerprint,
                turn_id: turn_id.clone(),
                ticket: ticket.clone(),
            },
        );
        SubmissionDecision::Accepted { turn_id, ticket }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definitively_unwritten_submission_can_be_registered_again() {
        let mut submissions = SubmissionMap::default();
        let first = match submissions.record("scope", "session", "submission", "hello", &[]) {
            SubmissionDecision::Accepted { ticket, .. } => ticket,
            other => panic!("首次提交必须被接受，实际为 {other:?}"),
        };

        assert!(first.claim_for_prompt());
        first.mark_not_written();

        assert!(matches!(
            submissions.record("scope", "session", "submission", "hello", &[]),
            SubmissionDecision::Accepted { .. }
        ));
    }

    #[test]
    fn uncertain_prompt_write_keeps_duplicate_guard() {
        let mut submissions = SubmissionMap::default();
        let ticket = match submissions.record("scope", "session", "submission", "hello", &[]) {
            SubmissionDecision::Accepted { ticket, .. } => ticket,
            other => panic!("首次提交必须被接受，实际为 {other:?}"),
        };

        assert!(ticket.claim_for_prompt());
        assert!(ticket.mark_may_have_been_written());
        assert!(matches!(
            submissions.record("scope", "session", "submission", "hello", &[]),
            SubmissionDecision::Duplicate { .. }
        ));
    }

    #[test]
    fn stale_ticket_forget_does_not_remove_replacement_submission() {
        let mut submissions = SubmissionMap::default();
        let first = match submissions.record("scope", "session", "submission", "hello", &[]) {
            SubmissionDecision::Accepted { ticket, .. } => ticket,
            other => panic!("首次提交必须被接受，实际为 {other:?}"),
        };
        assert!(first.claim_for_prompt());
        first.mark_not_written();

        let second = match submissions.record("scope", "session", "submission", "hello", &[]) {
            SubmissionDecision::Accepted { ticket, .. } => ticket,
            other => panic!("明确未写入后必须接受新一代 submission，实际为 {other:?}"),
        };
        submissions.forget("scope", "session", "submission", &first);
        assert_eq!(second.state(), SendTicketState::Waiting);
        assert!(matches!(
            submissions.record("scope", "session", "submission", "hello", &[]),
            SubmissionDecision::Duplicate { .. }
        ));
    }
}
