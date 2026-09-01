//! ACP `session/update` 到 Kit 产品事件的投影。
//!
//! Host 不把 ACP wire 类型泄漏到产品层，因此本模块只接收已经解码为
//! `serde_json::Value` 的通知参数。未知或禁用 update 必须降级计数，不能使
//! 一整批 live/replay 通知失败。

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use efflab_agent_contract::is_prompt_id;
use serde_json::{Map, Value};

use crate::{
    KIT_SCHEMA_VERSION, KitBlock, KitProductEvent, KitProductEventValidationError, Origin,
    ToolStatus,
};

/// ACP 标准 session update 的逻辑 method。
const SESSION_UPDATE_METHOD: &str = "session/update";

/// 以 scope 为边界维护会话投影状态。
///
/// 该状态只保存 UI 合并所需的快照、工具字段和内部计数；不保存原始 ACP
/// payload，避免把未知内容变成新的产品协议或意外留存敏感数据。
pub struct Projector {
    scope_id: String,
    sessions: BTreeMap<String, SessionProjection>,
    /// 没有稳定 sessionId 的未知/禁用通知只能安全跳过，仍需保留可观测计数。
    unattributed_skipped: u64,
}

impl fmt::Debug for Projector {
    /// 调试形状只暴露固定计数，绝不递归格式化含模型文本的 session 快照。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let replay_unsupported_updates = self.sessions.values().fold(0_u64, |total, session| {
            total.saturating_add(session.unsupported_replay_updates)
        });
        let live_unsupported_updates = self.sessions.values().fold(0_u64, |total, session| {
            total.saturating_add(session.unsupported_live_updates)
        });
        formatter
            .debug_struct("Projector")
            .field("session_count", &self.sessions.len())
            .field("unattributed_skipped", &self.unattributed_skipped)
            .field("unsupported_live_updates", &live_unsupported_updates)
            .field("unsupported_replay_updates", &replay_unsupported_updates)
            .finish()
    }
}

/// 单个 session 的增量投影状态。
#[derive(Default)]
struct SessionProjection {
    next_sequence: u64,
    replay_active: bool,
    /// live/replay 计数分别属于该 session actor，不进入 Kit 事件流。
    unsupported_live_updates: u64,
    unsupported_replay_updates: u64,
    active_prompt_id: Option<String>,
    assistant: Option<TextSnapshot>,
    thinking: Option<TextSnapshot>,
    tools: BTreeMap<String, ToolSnapshot>,
    /// 真实 ACP 流可能让 ToolCallUpdate 先于 ToolCall 到达，先暂存可覆盖字段。
    orphan_tool_updates: BTreeMap<String, PendingToolUpdate>,
}

/// 流式文本块的稳定 block_id、所属 prompt 与累计文本。
struct TextSnapshot {
    block_id: String,
    prompt_id: String,
    text: String,
}

/// ToolCall 与 ToolCallUpdate 合并后仍可展示的最小状态。
struct ToolSnapshot {
    name: String,
    detail: String,
    status: ToolStatus,
}

/// 区分状态字段的缺失、已知值和未知值，避免部分 update 覆盖已有状态。
#[derive(Clone, Copy)]
enum ToolStatusField {
    Absent,
    Known(ToolStatus),
    Unsupported,
}

/// 先于完整 ToolCall 到达的可选更新字段。
#[derive(Default)]
struct PendingToolUpdate {
    name: Option<String>,
    detail: Option<String>,
    status: Option<ToolStatus>,
}

impl PendingToolUpdate {
    /// 仅记录本条 update 实际出现的字段，保持 ACP 的部分更新语义。
    fn record(&mut self, title: Option<&str>, detail: Option<String>, status: ToolStatusField) {
        if let Some(title) = title {
            self.name = Some(title.to_string());
        }
        if let Some(detail) = detail {
            self.detail = Some(detail);
        }
        if let ToolStatusField::Known(status) = status {
            self.status = Some(status);
        }
    }

    /// 完整 ToolCall 迟到时，让此前实际出现的更新字段覆盖其基础字段。
    fn merge_into(self, tool: &mut ToolSnapshot) {
        if let Some(name) = self.name {
            tool.name = name;
        }
        if let Some(detail) = self.detail {
            tool.detail = detail;
        }
        if let Some(status) = self.status {
            tool.status = status;
        }
    }

    /// 没有完整 ToolCall 时也必须输出冻结 Tool 块，缺省字段采用安全占位。
    fn display_snapshot(&self) -> ToolSnapshot {
        ToolSnapshot {
            name: self.name.clone().unwrap_or_default(),
            detail: self.detail.clone().unwrap_or_default(),
            status: self.status.unwrap_or(ToolStatus::Pending),
        }
    }
}

/// 投影过程中仅用于已识别 update 的结构错误。
///
/// 未知、计划、todo 和 xAI session update 不使用本错误；它们必须走内部计数分支。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectError {
    /// 通知没有可用的 sessionId，无法生成任何 Kit 事件。
    MissingSessionId,
    /// 已识别的 turn 级 update 缺少 `_meta.promptId`。
    MissingPromptId { update_kind: &'static str },
    /// 已识别的文本 update 不含 `content.type=text` 与字符串 text。
    InvalidTextContent { update_kind: &'static str },
    /// 已识别的工具 update 缺少 toolCallId。
    MissingToolCallId,
    /// 每 session 序号耗尽，禁止回绕复用。
    SequenceExhausted,
    /// 防御性校验：任何待返回的 Kit 事件都必须先通过协议不变量。
    InvalidProductEvent(KitProductEventValidationError),
}

impl fmt::Display for ProjectError {
    /// 错误文本只描述固定协议字段，绝不回显 ACP payload。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSessionId => formatter.write_str("ACP 通知缺少有效 sessionId"),
            Self::MissingPromptId { update_kind } => {
                write!(formatter, "ACP {update_kind} 缺少有效 _meta.promptId")
            }
            Self::InvalidTextContent { update_kind } => {
                write!(formatter, "ACP {update_kind} 缺少 text content")
            }
            Self::MissingToolCallId => formatter.write_str("ACP 工具更新缺少有效 toolCallId"),
            Self::SequenceExhausted => formatter.write_str("ACP session sequence 已耗尽"),
            Self::InvalidProductEvent(error) => {
                write!(formatter, "projector 产出非法 KitProductEvent: {error:?}")
            }
        }
    }
}

impl Error for ProjectError {}

impl Projector {
    /// 为一个产品 scope 新建独立投影器。
    pub fn new(scope_id: impl Into<String>) -> Self {
        Self {
            scope_id: scope_id.into(),
            sessions: BTreeMap::new(),
            unattributed_skipped: 0,
        }
    }

    /// 返回没有稳定 sessionId 的未知/禁用通知累计跳过数。
    pub fn unattributed_skipped_count(&self) -> u64 {
        self.unattributed_skipped
    }

    /// 显式开始一次冷 replay；序号从零起，旧流式/工具快照不能跨栅栏复用。
    ///
    /// Task 7b 在写出 `session/load` 后调用本方法；为兼容遗漏该调用的旧接线，
    /// [`Self::apply_acp_notification`] 也会在首个 `_meta.isReplay=true` 通知处
    /// 自动建立相同栅栏。
    pub fn begin_replay(&mut self, session_id: &str) {
        self.sessions
            .entry(session_id.to_string())
            .or_default()
            .begin_replay();
    }

    /// 标记一个 prompt 的开始；重复调用同一 prompt 保留正在累计的文本快照。
    pub fn begin_prompt(&mut self, session_id: &str, prompt_id: &str) {
        self.sessions
            .entry(session_id.to_string())
            .or_default()
            .begin_prompt(prompt_id);
    }

    /// 读取当前 replay 批次的内部未知 update 计数，不结束 replay 栅栏。
    pub fn replay_skipped_count(&self, session_id: &str) -> u64 {
        self.sessions
            .get(session_id)
            .map_or(0, |session| session.unsupported_replay_updates)
    }

    /// 取得并清空 replay 内部未知 update 计数，同时结束当前 replay 栅栏。
    ///
    /// 本方法自身不构造任何产品事件；它保留旧 Host 接线所需的计数读取边界。
    pub fn take_replay_skipped_count(&mut self, session_id: &str) -> u64 {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return 0;
        };
        session.replay_active = false;
        session.active_prompt_id = None;
        session.clear_text_snapshots();
        std::mem::take(&mut session.unsupported_replay_updates)
    }

    /// 为 Host 合成的 session/process 状态分配同一会话的下一个稳定序号。
    ///
    /// 这样 `replay_complete`、`mcp_failed` 等事件不会与刚刚投影的 ACP update 复用
    /// sequence；新会话第一次合成状态从零开始。
    pub fn next_host_sequence(&mut self, session_id: &str) -> Result<u64, ProjectError> {
        self.sessions
            .entry(session_id.to_string())
            .or_default()
            .allocate_sequence()
    }

    /// 仅供 Host runtime 单元测试构造 sequence 耗尽边界；生产代码没有回退入口。
    #[cfg(test)]
    pub(crate) fn set_next_sequence_for_test(&mut self, session_id: &str, next_sequence: u64) {
        self.sessions
            .entry(session_id.to_string())
            .or_default()
            .next_sequence = next_sequence;
    }

    /// 应用一个 ACP notification 的 params，返回零条或多条已校验的 Kit 事件。
    pub fn apply_acp_notification(
        &mut self,
        method: &str,
        params: &Value,
    ) -> Result<Vec<KitProductEvent>, ProjectError> {
        let meta = params.get("_meta").and_then(Value::as_object);
        let origin = if meta
            .and_then(|meta| meta.get("isReplay"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            Origin::Replay
        } else {
            Origin::Live
        };
        // 先按 method/update kind 分类。未知、禁用或不完整的 update 即使缺少 sessionId
        // 也不能阻断整个 reader batch；只有可投影的已识别 turn update 才要求关联 session。
        let update = (method == SESSION_UPDATE_METHOD)
            .then(|| params.get("update").and_then(Value::as_object))
            .flatten();
        let update_kind =
            update.and_then(|update| update.get("sessionUpdate").and_then(Value::as_str));
        // 未知类型和已知类型中的不支持字段都在 sequence 分配前内部化。
        if !is_supported_update(update_kind, update) {
            let Some(session_id) = required_string(params, "sessionId") else {
                self.unattributed_skipped = self.unattributed_skipped.saturating_add(1);
                if should_log_skip(self.unattributed_skipped) {
                    tracing::debug!(
                        unattributed_skipped = self.unattributed_skipped,
                        origin = origin_name(origin),
                        "已安全跳过不可归属的 ACP 未知更新"
                    );
                }
                return Ok(Vec::new());
            };
            let session = self.sessions.entry(session_id.to_string()).or_default();
            // 未知通知是纯 no-op；先计数并返回，不能让 origin fence 清理快照或改动边界。
            return finish_events(skip_unknown_update(origin, session));
        }

        let session_id =
            required_string(params, "sessionId").ok_or(ProjectError::MissingSessionId)?;
        let Some(update) = update else {
            return Ok(Vec::new());
        };
        let Some(update_kind) = update_kind else {
            return Ok(Vec::new());
        };
        let scope_id = self.scope_id.clone();
        let session = self.sessions.entry(session_id.to_string()).or_default();
        session.prepare_origin(origin);
        let sequence = session.allocate_sequence()?;
        let event_id = sidecar_event_id(meta, session_id, origin, sequence);

        let events = match update_kind {
            "agent_message_chunk" => project_assistant(
                &scope_id, session_id, meta, origin, sequence, event_id, update, session,
            )?,
            "agent_thought_chunk" => project_thinking(
                &scope_id, session_id, meta, origin, sequence, event_id, update, session,
            )?,
            "tool_call" => project_tool_call(
                &scope_id, session_id, meta, origin, sequence, event_id, update, session,
            )?,
            "tool_call_update" => project_tool_call_update(
                &scope_id, session_id, meta, origin, sequence, event_id, update, session,
            )?,
            "user_message_chunk" => project_user_echo(
                &scope_id, session_id, meta, origin, sequence, event_id, update, session,
            )?,
            // 分类条件已在 sequence 分配前封闭；此分支仅作防御性安全降级。
            _ => skip_unknown_update(origin, session),
        };

        finish_events(events)
    }
}

/// 公开函数入口，便于 Host 读循环不依赖具体状态实现细节。
pub fn apply_acp_notification(
    projector: &mut Projector,
    method: &str,
    params: &Value,
) -> Result<Vec<KitProductEvent>, ProjectError> {
    projector.apply_acp_notification(method, params)
}

impl SessionProjection {
    /// 进入新的显式 replay 批次，清理跨历史边界的状态和旧 replay 计数。
    fn begin_replay(&mut self) {
        self.next_sequence = 0;
        self.replay_active = true;
        self.unsupported_replay_updates = 0;
        self.active_prompt_id = None;
        self.assistant = None;
        self.thinking = None;
        self.tools.clear();
        self.orphan_tool_updates.clear();
    }

    /// 为兼容遗漏显式边界的旧接线自动建立 replay fence，并保留 fence 前的计数。
    fn begin_replay_after_unknown(&mut self) {
        let replay_unknowns = self.unsupported_replay_updates;
        self.begin_replay();
        self.unsupported_replay_updates = replay_unknowns;
    }

    /// 根据通知来源建立 replay 栅栏；live 首包与 replay 首包不能共用流式快照。
    fn prepare_origin(&mut self, origin: Origin) {
        match origin {
            Origin::Replay if !self.replay_active => self.begin_replay_after_unknown(),
            Origin::Live if self.replay_active => {
                self.replay_active = false;
                self.active_prompt_id = None;
                self.clear_text_snapshots();
            }
            _ => {}
        }
    }

    /// 切换 prompt 时丢弃旧文本快照；同 prompt 重入保持累计内容。
    fn begin_prompt(&mut self, prompt_id: &str) {
        if self.active_prompt_id.as_deref() != Some(prompt_id) {
            self.active_prompt_id = Some(prompt_id.to_string());
            self.clear_text_snapshots();
        }
    }

    /// 按来源增加 actor-local 未知 update 计数，不触碰文本、工具或 sequence。
    fn record_unknown_update(&mut self, origin: Origin) -> u64 {
        let counter = match origin {
            Origin::Live => &mut self.unsupported_live_updates,
            Origin::Replay => &mut self.unsupported_replay_updates,
        };
        *counter = counter.saturating_add(1);
        *counter
    }

    /// 分配单调 sequence；溢出时拒绝回绕。
    fn allocate_sequence(&mut self) -> Result<u64, ProjectError> {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ProjectError::SequenceExhausted)?;
        Ok(sequence)
    }

    /// 文本与工具边界会切断当前流式文本块，防止跨 block 累计。
    fn clear_text_snapshots(&mut self) {
        self.assistant = None;
        self.thinking = None;
    }
}

/// 投影 agent_message_chunk；每个连续块向产品发累计快照。
#[allow(clippy::too_many_arguments)]
fn project_assistant(
    scope_id: &str,
    session_id: &str,
    meta: Option<&Map<String, Value>>,
    origin: Origin,
    sequence: u64,
    event_id: String,
    update: &Map<String, Value>,
    session: &mut SessionProjection,
) -> Result<Vec<KitProductEvent>, ProjectError> {
    let prompt_id = required_prompt_id(meta, "agent_message_chunk")?;
    let text = required_text(update, "agent_message_chunk")?;
    session.begin_prompt(prompt_id);
    // 新的 assistant 文本会结束上一段 thinking，连续 assistant chunk 继续同一快照。
    session.thinking = None;
    let snapshot = session.assistant.get_or_insert_with(|| TextSnapshot {
        block_id: event_id.clone(),
        prompt_id: prompt_id.to_string(),
        text: String::new(),
    });
    if snapshot.prompt_id != prompt_id {
        *snapshot = TextSnapshot {
            block_id: event_id.clone(),
            prompt_id: prompt_id.to_string(),
            text: String::new(),
        };
    }
    snapshot.text.push_str(text);

    Ok(vec![turn_event(
        scope_id,
        session_id,
        prompt_id,
        event_id,
        sequence,
        origin,
        snapshot.block_id.clone(),
        KitBlock::Assistant {
            markdown: snapshot.text.clone(),
            streaming: origin == Origin::Live,
        },
    )])
}

/// 投影 agent_thought_chunk；同一连续 thought 块也保留稳定 block_id 与累计文本。
#[allow(clippy::too_many_arguments)]
fn project_thinking(
    scope_id: &str,
    session_id: &str,
    meta: Option<&Map<String, Value>>,
    origin: Origin,
    sequence: u64,
    event_id: String,
    update: &Map<String, Value>,
    session: &mut SessionProjection,
) -> Result<Vec<KitProductEvent>, ProjectError> {
    let prompt_id = required_prompt_id(meta, "agent_thought_chunk")?;
    let text = required_text(update, "agent_thought_chunk")?;
    session.begin_prompt(prompt_id);
    // 思考开始后，后续 assistant chunk 必须成为新的展示块。
    session.assistant = None;
    let snapshot = session.thinking.get_or_insert_with(|| TextSnapshot {
        block_id: event_id.clone(),
        prompt_id: prompt_id.to_string(),
        text: String::new(),
    });
    if snapshot.prompt_id != prompt_id {
        *snapshot = TextSnapshot {
            block_id: event_id.clone(),
            prompt_id: prompt_id.to_string(),
            text: String::new(),
        };
    }
    snapshot.text.push_str(text);

    Ok(vec![turn_event(
        scope_id,
        session_id,
        prompt_id,
        event_id,
        sequence,
        origin,
        snapshot.block_id.clone(),
        KitBlock::Thinking {
            text: snapshot.text.clone(),
        },
    )])
}

/// 投影完整 tool_call，并保留字段给随后的顶层 tool_call_update 合并。
#[allow(clippy::too_many_arguments)]
fn project_tool_call(
    scope_id: &str,
    session_id: &str,
    meta: Option<&Map<String, Value>>,
    origin: Origin,
    sequence: u64,
    event_id: String,
    update: &Map<String, Value>,
    session: &mut SessionProjection,
) -> Result<Vec<KitProductEvent>, ProjectError> {
    let prompt_id = required_prompt_id(meta, "tool_call")?;
    let tool_call_id =
        required_string_from_map(update, "toolCallId").ok_or(ProjectError::MissingToolCallId)?;
    let status = match tool_status(update.get("status")) {
        ToolStatusField::Known(status) => status,
        ToolStatusField::Absent => ToolStatus::Pending,
        ToolStatusField::Unsupported => return Ok(skip_unknown_update(origin, session)),
    };
    session.begin_prompt(prompt_id);
    session.clear_text_snapshots();
    let mut tool = ToolSnapshot {
        name: optional_string(update, "title")
            .unwrap_or_default()
            .to_string(),
        detail: tool_detail(update).unwrap_or_default(),
        status,
    };
    // codegen 的 tracker 明确处理 update 先到的竞态；已出现字段覆盖迟到的基础调用。
    if let Some(orphan) = session.orphan_tool_updates.remove(tool_call_id) {
        orphan.merge_into(&mut tool);
    }
    let block = tool_block(tool_call_id, &tool);
    session.tools.insert(tool_call_id.to_string(), tool);

    Ok(vec![turn_event(
        scope_id,
        session_id,
        prompt_id,
        event_id,
        sequence,
        origin,
        tool_call_id.to_string(),
        block,
    )])
}

/// 投影部分 tool_call_update；codegen wire 将可选 fields 平铺在 update 顶层。
#[allow(clippy::too_many_arguments)]
fn project_tool_call_update(
    scope_id: &str,
    session_id: &str,
    meta: Option<&Map<String, Value>>,
    origin: Origin,
    sequence: u64,
    event_id: String,
    update: &Map<String, Value>,
    session: &mut SessionProjection,
) -> Result<Vec<KitProductEvent>, ProjectError> {
    let prompt_id = required_prompt_id(meta, "tool_call_update")?;
    let tool_call_id =
        required_string_from_map(update, "toolCallId").ok_or(ProjectError::MissingToolCallId)?;
    let parsed_status = tool_status(update.get("status"));
    if matches!(parsed_status, ToolStatusField::Unsupported) {
        return Ok(skip_unknown_update(origin, session));
    }
    session.begin_prompt(prompt_id);
    session.clear_text_snapshots();
    let title = optional_string(update, "title");
    let detail = tool_detail(update);
    let block = if let Some(tool) = session.tools.get_mut(tool_call_id) {
        if let Some(title) = title {
            tool.name = title.to_string();
        }
        if let Some(detail) = detail {
            tool.detail = detail;
        }
        if let ToolStatusField::Known(status) = parsed_status {
            tool.status = status;
        }
        tool_block(tool_call_id, tool)
    } else {
        // 尚无完整 ToolCall 时保留逐字段覆盖信息，供迟到的基础调用合并。
        let orphan = session
            .orphan_tool_updates
            .entry(tool_call_id.to_string())
            .or_default();
        orphan.record(title, detail, parsed_status);
        let tool = orphan.display_snapshot();
        tool_block(tool_call_id, &tool)
    };

    Ok(vec![turn_event(
        scope_id,
        session_id,
        prompt_id,
        event_id,
        sequence,
        origin,
        tool_call_id.to_string(),
        block,
    )])
}

/// 投影 user_message_chunk；promptId 优先成为 block_id，避免与乐观 user 气泡重复。
#[allow(clippy::too_many_arguments)]
fn project_user_echo(
    scope_id: &str,
    session_id: &str,
    meta: Option<&Map<String, Value>>,
    origin: Origin,
    sequence: u64,
    event_id: String,
    update: &Map<String, Value>,
    session: &mut SessionProjection,
) -> Result<Vec<KitProductEvent>, ProjectError> {
    let prompt_id = required_prompt_id(meta, "user_message_chunk")?;
    let text = required_text(update, "user_message_chunk")?;
    session.begin_prompt(prompt_id);
    session.clear_text_snapshots();

    Ok(vec![turn_event(
        scope_id,
        session_id,
        prompt_id,
        event_id,
        sequence,
        origin,
        prompt_id.to_string(),
        KitBlock::User {
            text: text.to_string(),
        },
    )])
}

/// 未知更新的 fail-open 分支：只保留 actor-local 计数，不生成产品事件。
fn skip_unknown_update(origin: Origin, session: &mut SessionProjection) -> Vec<KitProductEvent> {
    let count = session.record_unknown_update(origin);
    if should_log_skip(count) {
        tracing::debug!(
            origin = origin_name(origin),
            unsupported_update_total = count,
            "已内部化不支持的 ACP 更新"
        );
    }
    Vec::new()
}

/// 以首条和指数间隔记录未知更新，避免异常 sidecar 刷屏。
fn should_log_skip(count: u64) -> bool {
    count == 1 || (count.is_power_of_two() && count < u64::MAX)
}

/// 只把已支持的 ACP update 放入序号分配后的投影路径。
fn is_supported_update(update_kind: Option<&str>, update: Option<&Map<String, Value>>) -> bool {
    let recognized = matches!(
        update_kind,
        Some(
            "agent_message_chunk"
                | "agent_thought_chunk"
                | "tool_call"
                | "tool_call_update"
                | "user_message_chunk"
        )
    );
    if !recognized {
        return false;
    }

    // 工具状态 token 不在冻结的 Kit 五值内时，整条 update 也必须内部化。
    if matches!(update_kind, Some("tool_call" | "tool_call_update"))
        && matches!(
            tool_status(update.and_then(|update| update.get("status"))),
            ToolStatusField::Unsupported
        )
    {
        return false;
    }

    true
}

/// 构造 turn 级事件；turn_id 与 submission_id 必须同时等于 ACP promptId。
#[allow(clippy::too_many_arguments)]
fn turn_event(
    scope_id: &str,
    session_id: &str,
    prompt_id: &str,
    event_id: String,
    sequence: u64,
    origin: Origin,
    block_id: String,
    block: KitBlock,
) -> KitProductEvent {
    KitProductEvent {
        schema_version: KIT_SCHEMA_VERSION,
        scope_id: scope_id.to_string(),
        session_id: session_id.to_string(),
        turn_id: Some(prompt_id.to_string()),
        submission_id: Some(prompt_id.to_string()),
        event_id,
        sequence,
        origin,
        block_id,
        block,
    }
}

/// 将内部 tool snapshot 转成冻结的 KitBlock。
fn tool_block(tool_call_id: &str, tool: &ToolSnapshot) -> KitBlock {
    KitBlock::Tool {
        tool_call_id: tool_call_id.to_string(),
        name: tool.name.clone(),
        detail: tool.detail.clone(),
        status: tool.status,
    }
}

/// 仅接受冻结 Kit 支持的工具状态；未知 token 整体按未知 update 跳过。
fn tool_status(value: Option<&Value>) -> ToolStatusField {
    let Some(value) = value else {
        return ToolStatusField::Absent;
    };
    let Some(status) = value.as_str() else {
        return ToolStatusField::Unsupported;
    };
    match status {
        "pending" => ToolStatusField::Known(ToolStatus::Pending),
        // ACP 的真实 token 是 in_progress；Kit 对产品固定为 running。
        "in_progress" => ToolStatusField::Known(ToolStatus::Running),
        "completed" => ToolStatusField::Known(ToolStatus::Completed),
        "failed" => ToolStatusField::Known(ToolStatus::Failed),
        "cancelled" => ToolStatusField::Known(ToolStatus::Cancelled),
        _ => ToolStatusField::Unsupported,
    }
}

/// 从 tool content 数组安全提取文本 detail；非文本字段有意不转发。
fn tool_detail(update: &Map<String, Value>) -> Option<String> {
    update.get("content").map(text_from_tool_content)
}

/// ToolCall 的 content 是 ContentBlock 数组；只合并 `type=text` 块。
fn text_from_tool_content(value: &Value) -> String {
    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(text_from_content_block)
            .collect::<Vec<_>>()
            .join(""),
        _ => text_from_content_block(value).unwrap_or_default(),
    }
}

/// 从标准 ContentBlock 取字符串 text，拒绝 image/resource 等非文本内容。
fn text_from_content_block(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    (object.get("type").and_then(Value::as_str) == Some("text"))
        .then(|| {
            object
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .flatten()
}

/// 已识别文本 update 必须使用 ACP ContentChunk 的 text block 形状。
fn required_text<'a>(
    update: &'a Map<String, Value>,
    update_kind: &'static str,
) -> Result<&'a str, ProjectError> {
    update
        .get("content")
        .and_then(Value::as_object)
        .filter(|content| content.get("type").and_then(Value::as_str) == Some("text"))
        .and_then(|content| content.get("text").and_then(Value::as_str))
        .ok_or(ProjectError::InvalidTextContent { update_kind })
}

/// 取得 turn 级更新必须携带的 sidecar promptId。
fn required_prompt_id<'a>(
    meta: Option<&'a Map<String, Value>>,
    update_kind: &'static str,
) -> Result<&'a str, ProjectError> {
    meta.and_then(|meta| meta.get("promptId"))
        .and_then(Value::as_str)
        .filter(|prompt_id| is_prompt_id(prompt_id))
        .ok_or(ProjectError::MissingPromptId { update_kind })
}

/// 读取 params 或 update object 中的非空字符串字段。
fn required_string<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

/// 读取 map 中的非空字符串字段。
fn required_string_from_map<'a>(map: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    map.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

/// 读取 map 中允许为空的可选字符串字段。
fn optional_string<'a>(map: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    map.get(field).and_then(Value::as_str)
}

/// sidecar eventId 优先；缺失时按冻结 session/origin/sequence 规则派生。
fn sidecar_event_id(
    meta: Option<&Map<String, Value>>,
    session_id: &str,
    origin: Origin,
    sequence: u64,
) -> String {
    meta.and_then(|meta| meta.get("eventId"))
        .and_then(Value::as_str)
        .filter(|event_id| !event_id.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("{session_id}:{}:{sequence}", origin_name(origin)))
}

/// 事件 ID 回退值使用产品协议中固定的小写 origin token。
fn origin_name(origin: Origin) -> &'static str {
    match origin {
        Origin::Live => "live",
        Origin::Replay => "replay",
    }
}

/// 在任何事件离开 projector 前执行 KitProductEvent 的冻结不变量校验。
fn finish_events(events: Vec<KitProductEvent>) -> Result<Vec<KitProductEvent>, ProjectError> {
    for event in &events {
        event
            .validate()
            .map_err(ProjectError::InvalidProductEvent)?;
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use tracing::{Event, Metadata, Subscriber, field, span};

    use super::{Projector, should_log_skip};

    /// 限频只允许首条和 2 的幂次日志，明确覆盖 1/2/3/4 边界。
    #[test]
    fn should_log_skip_covers_first_power_and_non_power_boundaries() {
        assert!(should_log_skip(1));
        assert!(should_log_skip(2));
        assert!(!should_log_skip(3));
        assert!(should_log_skip(4));
    }

    /// 构造携带测试秘密的未知通知，验证日志不会格式化原始 ACP payload。
    fn unknown_notification(session_id: &str, is_replay: bool) -> Value {
        json!({
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "future_tool_call_update_secret",
                "status": { "secret": "status-payload-secret" },
                "content": [{ "type": "text", "text": "payload-secret" }],
                "privateField": "field-secret"
            },
            "_meta": {
                "promptId": "prompt-secret",
                "isReplay": is_replay,
                "eventId": "event-secret"
            }
        })
    }

    /// 只捕获结构化 tracing 字段，避免测试依赖未声明的 tracing-subscriber。
    #[derive(Clone, Default)]
    struct LogCapture {
        records: Arc<Mutex<Vec<LogRecord>>>,
    }

    #[derive(Clone, Debug)]
    struct LogRecord {
        fields: BTreeMap<String, String>,
    }

    impl LogCapture {
        /// 复制捕获结果，使 subscriber guard 可以在断言前释放。
        fn records(&self) -> Vec<LogRecord> {
            self.records.lock().expect("日志捕获锁不应中毒").clone()
        }
    }

    struct FieldCapture {
        fields: BTreeMap<String, String>,
    }

    impl field::Visit for FieldCapture {
        /// 固定保存 Debug 字段，message 等格式化字段不会展开嵌套 payload。
        fn record_debug(&mut self, field: &field::Field, value: &dyn fmt::Debug) {
            self.fields
                .insert(field.name().to_string(), format!("{value:?}"));
        }

        /// 保留字符串字段原值，便于断言 origin 白名单。
        fn record_str(&mut self, field: &field::Field, value: &str) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }

        /// 保留计数字段的十进制表示，便于断言限频边界。
        fn record_u64(&mut self, field: &field::Field, value: u64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    impl Subscriber for LogCapture {
        /// 仅接收 debug 级别事件，覆盖 projector 的限频日志调用。
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            *metadata.level() <= tracing::Level::DEBUG
        }

        /// 测试不创建 span，返回固定有效 ID 满足 Subscriber 接口。
        fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }

        /// projector 不记录 span 字段，测试 subscriber 也不保留它们。
        fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

        /// projector 不建立 span 因果关系。
        fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

        /// 记录结构化事件字段，不格式化任何外部 ACP 参数。
        fn event(&self, event: &Event<'_>) {
            let mut visitor = FieldCapture {
                fields: BTreeMap::new(),
            };
            event.record(&mut visitor);
            self.records
                .lock()
                .expect("日志捕获锁不应中毒")
                .push(LogRecord {
                    fields: visitor.fields,
                });
        }

        /// projector 测试不进入 span。
        fn enter(&self, _span: &span::Id) {}

        /// projector 测试不退出 span。
        fn exit(&self, _span: &span::Id) {}
    }

    /// 多 session、双 origin 的计数必须隔离，限频日志必须脱敏且只含固定字段。
    #[test]
    fn unsupported_counters_are_session_origin_local_and_logs_are_redacted() {
        let capture = LogCapture::default();
        let mut projector = Projector::new("scope-1");
        let records = {
            let _guard = tracing::subscriber::set_default(capture.clone());
            for _ in 0..4 {
                assert!(
                    projector
                        .apply_acp_notification(
                            "session/update",
                            &unknown_notification("session-a", false),
                        )
                        .expect("live 未知通知必须安全忽略")
                        .is_empty()
                );
            }
            assert!(
                projector
                    .apply_acp_notification(
                        "session/update",
                        &unknown_notification("session-b", false),
                    )
                    .expect("另一个 live session 的未知通知必须安全忽略")
                    .is_empty()
            );

            projector.begin_replay("session-a");
            for _ in 0..4 {
                assert!(
                    projector
                        .apply_acp_notification(
                            "session/update",
                            &unknown_notification("session-a", true),
                        )
                        .expect("replay 未知通知必须安全忽略")
                        .is_empty()
                );
            }
            projector.begin_replay("session-b");
            assert!(
                projector
                    .apply_acp_notification(
                        "session/update",
                        &unknown_notification("session-b", true),
                    )
                    .expect("另一个 replay session 的未知通知必须安全忽略")
                    .is_empty()
            );
            capture.records()
        };

        let session_a = projector
            .sessions
            .get("session-a")
            .expect("session-a 应存在");
        let session_b = projector
            .sessions
            .get("session-b")
            .expect("session-b 应存在");
        assert_eq!(session_a.unsupported_live_updates, 4);
        assert_eq!(session_a.unsupported_replay_updates, 4);
        assert_eq!(session_b.unsupported_live_updates, 1);
        assert_eq!(session_b.unsupported_replay_updates, 1);

        let origins_and_counts: Vec<(&str, u64)> = records
            .iter()
            .map(|record| {
                let origin = record
                    .fields
                    .get("origin")
                    .map(String::as_str)
                    .expect("日志必须包含固定 origin 字段");
                let count = record
                    .fields
                    .get("unsupported_update_total")
                    .expect("日志必须包含固定 count 字段")
                    .parse()
                    .expect("日志 count 必须是十进制整数");
                (origin, count)
            })
            .collect();
        assert_eq!(
            origins_and_counts,
            vec![
                ("live", 1),
                ("live", 2),
                ("live", 4),
                ("live", 1),
                ("replay", 1),
                ("replay", 2),
                ("replay", 4),
                ("replay", 1),
            ]
        );
        for record in records {
            assert_eq!(
                record.fields.keys().map(String::as_str).collect::<Vec<_>>(),
                vec!["message", "origin", "unsupported_update_total"]
            );
            assert_eq!(
                record.fields.get("message").map(String::as_str),
                Some("已内部化不支持的 ACP 更新")
            );
            let serialized = format!("{:?}", record.fields);
            assert!(!serialized.contains("secret"));
            assert!(!serialized.contains("future_tool_call_update"));
            assert!(!serialized.contains("payload"));
            assert!(!serialized.contains("privateField"));
        }
    }
}
