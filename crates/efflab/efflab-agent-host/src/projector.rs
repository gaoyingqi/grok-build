//! ACP `session/update` 到 Kit 产品事件的投影。
//!
//! Host 不把 ACP wire 类型泄漏到产品层，因此本模块只接收已经解码为
//! `serde_json::Value` 的通知参数。未知或禁用 update 必须降级计数，不能使
//! 一整批 live/replay 通知失败。

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use serde_json::{Map, Value};

use crate::{
    KIT_SCHEMA_VERSION, KitBlock, KitProductEvent, KitProductEventValidationError, Origin,
    ToolStatus,
};

/// ACP 标准 session update 的逻辑 method。
const SESSION_UPDATE_METHOD: &str = "session/update";

/// 以 scope 为边界维护会话投影状态。
///
/// 该状态只保存 UI 合并所需的快照、工具字段和 replay 跳过计数；不保存原始
/// ACP payload，避免把未知内容变成新的产品协议或意外留存敏感数据。
#[derive(Debug)]
pub struct Projector {
    scope_id: String,
    sessions: BTreeMap<String, SessionProjection>,
}

/// 单个 session 的增量投影状态。
#[derive(Debug, Default)]
struct SessionProjection {
    next_sequence: u64,
    replay_active: bool,
    replay_skipped: u64,
    assistant: Option<TextSnapshot>,
    thinking: Option<TextSnapshot>,
    tools: BTreeMap<String, ToolSnapshot>,
    /// 真实 ACP 流可能让 ToolCallUpdate 先于 ToolCall 到达，先暂存可覆盖字段。
    orphan_tool_updates: BTreeMap<String, PendingToolUpdate>,
}

/// 流式文本块的稳定 block_id 与累计文本。
#[derive(Debug)]
struct TextSnapshot {
    block_id: String,
    text: String,
}

/// ToolCall 与 ToolCallUpdate 合并后仍可展示的最小状态。
#[derive(Debug)]
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
#[derive(Debug, Default)]
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
/// 未知、计划、todo 和 xAI session update 不使用本错误；它们必须走
/// `skipped_update` 或 replay 计数分支。
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
        }
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

    /// 读取当前 replay 批次已跳过的 update 数量，不结束 replay 栅栏。
    pub fn replay_skipped_count(&self, session_id: &str) -> u64 {
        self.sessions
            .get(session_id)
            .map_or(0, |session| session.replay_skipped)
    }

    /// 取得并清空 replay 跳过数，同时结束当前 replay 栅栏。
    ///
    /// 本方法故意不构造 `replay_skipped` 或 `replay_complete` 事件；其发射时机
    /// 依赖 `session/load` response 排空，属于 Task 7b 的职责。
    pub fn take_replay_skipped_count(&mut self, session_id: &str) -> u64 {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return 0;
        };
        session.replay_active = false;
        std::mem::take(&mut session.replay_skipped)
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

    /// 应用一个 ACP notification 的 params，返回零条或多条已校验的 Kit 事件。
    pub fn apply_acp_notification(
        &mut self,
        method: &str,
        params: &Value,
    ) -> Result<Vec<KitProductEvent>, ProjectError> {
        let session_id =
            required_string(params, "sessionId").ok_or(ProjectError::MissingSessionId)?;
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
        let scope_id = self.scope_id.clone();
        let session = self.sessions.entry(session_id.to_string()).or_default();
        session.prepare_origin(origin);
        let sequence = session.allocate_sequence()?;
        let event_id = sidecar_event_id(meta, session_id, origin, sequence);

        // 仅标准 session/update 可投影；xAI 扩展和其他 method 一律安全跳过。
        if method != SESSION_UPDATE_METHOD {
            return finish_events(skip_unknown_update(
                &scope_id, session_id, origin, sequence, session,
            ));
        }

        let Some(update) = params.get("update").and_then(Value::as_object) else {
            return finish_events(skip_unknown_update(
                &scope_id, session_id, origin, sequence, session,
            ));
        };
        let Some(update_kind) = update.get("sessionUpdate").and_then(Value::as_str) else {
            return finish_events(skip_unknown_update(
                &scope_id, session_id, origin, sequence, session,
            ));
        };

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
            // plan / todo 在 M1 没有 KitBlock，未知未来变体也必须 fail-open。
            _ => skip_unknown_update(&scope_id, session_id, origin, sequence, session),
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
    /// 进入 replay 前清理会跨历史边界混淆的缓存，并把 sequence 重置为零。
    fn begin_replay(&mut self) {
        self.next_sequence = 0;
        self.replay_active = true;
        self.replay_skipped = 0;
        self.assistant = None;
        self.thinking = None;
        self.tools.clear();
        self.orphan_tool_updates.clear();
    }

    /// 根据通知来源建立 replay 栅栏；live 首包与 replay 首包不能共用流式快照。
    fn prepare_origin(&mut self, origin: Origin) {
        match origin {
            Origin::Replay if !self.replay_active => self.begin_replay(),
            Origin::Live if self.replay_active => {
                self.replay_active = false;
                self.clear_text_snapshots();
            }
            _ => {}
        }
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
    // 新的 assistant 文本会结束上一段 thinking，连续 assistant chunk 继续同一快照。
    session.thinking = None;
    let snapshot = session.assistant.get_or_insert_with(|| TextSnapshot {
        block_id: event_id.clone(),
        text: String::new(),
    });
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
    // 思考开始后，后续 assistant chunk 必须成为新的展示块。
    session.assistant = None;
    let snapshot = session.thinking.get_or_insert_with(|| TextSnapshot {
        block_id: event_id.clone(),
        text: String::new(),
    });
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
        ToolStatusField::Unsupported => {
            return Ok(skip_unknown_update(
                scope_id, session_id, origin, sequence, session,
            ));
        }
    };
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
        return Ok(skip_unknown_update(
            scope_id, session_id, origin, sequence, session,
        ));
    }
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

/// 未知更新的 fail-open 分支：replay 只计数，live 发 session 级 Status。
fn skip_unknown_update(
    scope_id: &str,
    session_id: &str,
    origin: Origin,
    sequence: u64,
    session: &mut SessionProjection,
) -> Vec<KitProductEvent> {
    session.clear_text_snapshots();
    if origin == Origin::Replay {
        session.replay_skipped = session.replay_skipped.saturating_add(1);
        return Vec::new();
    }

    let code = "skipped_update";
    let event_id = format!("{session_id}:host:{code}:{sequence}");
    vec![KitProductEvent {
        schema_version: KIT_SCHEMA_VERSION,
        scope_id: scope_id.to_string(),
        session_id: session_id.to_string(),
        turn_id: None,
        submission_id: None,
        event_id: event_id.clone(),
        sequence,
        origin,
        block_id: event_id,
        // 不包含原始 update 名或 payload，避免未知内容进入产品显示与日志。
        block: KitBlock::Status {
            code: code.to_string(),
            message: "已跳过不支持的 sidecar 更新".to_string(),
        },
    }]
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
    match value.and_then(Value::as_str) {
        None => ToolStatusField::Absent,
        Some("pending") => ToolStatusField::Known(ToolStatus::Pending),
        // ACP 的真实 token 是 in_progress；Kit 对产品固定为 running。
        Some("in_progress") => ToolStatusField::Known(ToolStatus::Running),
        Some("completed") => ToolStatusField::Known(ToolStatus::Completed),
        Some("failed") => ToolStatusField::Known(ToolStatus::Failed),
        Some("cancelled") => ToolStatusField::Known(ToolStatus::Cancelled),
        Some(_) => ToolStatusField::Unsupported,
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
        .filter(|prompt_id| !prompt_id.is_empty())
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
