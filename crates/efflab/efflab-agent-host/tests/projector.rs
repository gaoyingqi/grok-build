//! ACP 通知到 Kit 产品事件的投影测试。
//!
//! 所有 fixture 沿用 codegen 的真实 wire 形状：`sessionId`、内标
//! `sessionUpdate`、`toolCallId` 与顶层 `status`，避免测试自己发明 ACP 字段。

use efflab_agent_host::{KitBlock, Origin, Projector, ToolStatus, apply_acp_notification};
use serde_json::{Value, json};

/// 使用 codegen `agent/update_chunk_merge.rs` 的 `SessionNotification` wire 形状构造文本块。
fn text_notification(update_kind: &str, text: &str, meta: Value) -> Value {
    json!({
        "sessionId": "session-1",
        "update": {
            "sessionUpdate": update_kind,
            "content": { "type": "text", "text": text }
        },
        "_meta": meta,
    })
}

/// 使用 codegen `app/subagent.rs` 的 tool_call / tool_call_update 顶层字段形状构造通知。
fn tool_notification(update: Value, meta: Value) -> Value {
    json!({
        "sessionId": "session-1",
        "update": update,
        "_meta": meta,
    })
}

/// 所有从 projector 返回的事件都必须在生产边界校验前已满足标识不变量。
fn assert_valid(events: &[efflab_agent_host::KitProductEvent]) {
    for event in events {
        event
            .validate()
            .expect("projector 产出的 KitProductEvent 必须通过 validate");
    }
}

/// 多个 assistant delta 必须共享 block_id，并以累计 Markdown 快照而非 delta 发出。
#[test]
fn assistant_chunks_accumulate_into_a_single_streaming_snapshot() {
    let mut projector = Projector::new("scope-1");
    let first = text_notification(
        "agent_message_chunk",
        "he",
        json!({ "eventId": "sidecar-event-1", "promptId": "turn-1" }),
    );
    let second = text_notification(
        "agent_message_chunk",
        "llo",
        json!({ "eventId": "sidecar-event-2", "promptId": "turn-1" }),
    );

    let first_events = apply_acp_notification(&mut projector, "session/update", &first)
        .expect("第一条 assistant chunk 必须可投影");
    let second_events = apply_acp_notification(&mut projector, "session/update", &second)
        .expect("第二条 assistant chunk 必须可投影");

    assert_valid(&first_events);
    assert_valid(&second_events);
    assert_eq!(first_events.len(), 1);
    assert_eq!(second_events.len(), 1);
    assert_eq!(first_events[0].event_id, "sidecar-event-1");
    assert_eq!(first_events[0].turn_id.as_deref(), Some("turn-1"));
    assert_eq!(first_events[0].submission_id.as_deref(), Some("turn-1"));
    assert_eq!(first_events[0].origin, Origin::Live);
    assert!(matches!(
        &first_events[0].block,
        KitBlock::Assistant { markdown, streaming } if markdown == "he" && *streaming
    ));
    assert!(matches!(
        &second_events[0].block,
        KitBlock::Assistant { markdown, streaming } if markdown == "hello" && *streaming
    ));
    assert_eq!(first_events[0].block_id, second_events[0].block_id);
}

/// 思考块、用户回显和工具调用均必须使用真实 ACP 字段并映射为冻结 KitBlock。
#[test]
fn maps_thinking_tool_and_user_echo_from_real_acp_shapes() {
    let mut projector = Projector::new("scope-1");
    let meta = json!({ "promptId": "turn-1" });
    let thinking = text_notification("agent_thought_chunk", "先检查输入", meta.clone());
    let tool_call = tool_notification(
        json!({
            "sessionUpdate": "tool_call",
            "toolCallId": "tool-1",
            "title": "bash",
            "kind": "execute",
            "status": "pending"
        }),
        meta.clone(),
    );
    // `status` 与 `content` 是 tool_call_update 的顶层字段，不是虚构的 fields 包装。
    let tool_update = tool_notification(
        json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "tool-1",
            "status": "in_progress",
            "content": [{ "type": "text", "text": "running" }]
        }),
        meta.clone(),
    );
    let user = text_notification("user_message_chunk", "用户问题", meta);

    let thinking_events = apply_acp_notification(&mut projector, "session/update", &thinking)
        .expect("thinking 必须可投影");
    let tool_events = apply_acp_notification(&mut projector, "session/update", &tool_call)
        .expect("tool_call 必须可投影");
    let update_events = apply_acp_notification(&mut projector, "session/update", &tool_update)
        .expect("tool_call_update 必须可投影");
    let user_events = apply_acp_notification(&mut projector, "session/update", &user)
        .expect("user echo 必须可投影");

    assert_valid(&thinking_events);
    assert_valid(&tool_events);
    assert_valid(&update_events);
    assert_valid(&user_events);
    assert!(matches!(
        &thinking_events[0].block,
        KitBlock::Thinking { text } if text == "先检查输入"
    ));
    assert!(matches!(
        &tool_events[0].block,
        KitBlock::Tool { tool_call_id, name, detail, status }
            if tool_call_id == "tool-1" && name == "bash" && detail.is_empty()
                && *status == ToolStatus::Pending
    ));
    assert!(matches!(
        &update_events[0].block,
        KitBlock::Tool { tool_call_id, name, detail, status }
            if tool_call_id == "tool-1" && name == "bash" && detail == "running"
                && *status == ToolStatus::Running
    ));
    assert!(matches!(
        &user_events[0].block,
        KitBlock::User { text } if text == "用户问题"
    ));
    assert_eq!(user_events[0].block_id, "turn-1");
}

/// ACP 的 tool_call_update 是部分更新；省略 status 时必须保留已知工具状态。
#[test]
fn tool_call_update_without_status_preserves_previous_status() {
    let mut projector = Projector::new("scope-1");
    let meta = json!({ "promptId": "turn-1" });
    let tool_call = tool_notification(
        json!({
            "sessionUpdate": "tool_call",
            "toolCallId": "tool-1",
            "title": "bash",
            "status": "in_progress"
        }),
        meta.clone(),
    );
    let partial_update = tool_notification(
        json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "tool-1",
            "content": [{ "type": "text", "text": "still running" }]
        }),
        meta,
    );

    let _ = apply_acp_notification(&mut projector, "session/update", &tool_call)
        .expect("初始工具调用必须可投影");
    let events = apply_acp_notification(&mut projector, "session/update", &partial_update)
        .expect("部分工具更新必须可投影");

    assert_valid(&events);
    assert!(matches!(
        &events[0].block,
        KitBlock::Tool { status, detail, .. }
            if *status == ToolStatus::Running && detail == "still running"
    ));
}

/// ACP 可出现 ToolCallUpdate 先于 ToolCall；完整调用到达时必须合并已见的更新字段。
#[test]
fn tool_call_after_update_preserves_orphan_update_fields() {
    let mut projector = Projector::new("scope-1");
    let meta = json!({ "promptId": "turn-1" });
    let update_first = tool_notification(
        json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "tool-1",
            "status": "in_progress",
            "content": [{ "type": "text", "text": "streamed output" }]
        }),
        meta.clone(),
    );
    let base_later = tool_notification(
        json!({
            "sessionUpdate": "tool_call",
            "toolCallId": "tool-1",
            "title": "bash",
            "status": "pending"
        }),
        meta,
    );

    let _ = apply_acp_notification(&mut projector, "session/update", &update_first)
        .expect("先到的工具更新必须可投影");
    let events = apply_acp_notification(&mut projector, "session/update", &base_later)
        .expect("后到的完整工具调用必须合并已见更新");

    assert_valid(&events);
    assert!(matches!(
        &events[0].block,
        KitBlock::Tool { name, detail, status, .. }
            if name == "bash" && detail == "streamed output" && *status == ToolStatus::Running
    ));
}

/// replay 的文本必须停用流式渲染，且 replay 栅栏从 sequence=0 开始。
#[test]
fn replay_marks_origin_disables_streaming_and_resets_sequence() {
    let mut projector = Projector::new("scope-1");
    let live = text_notification(
        "agent_message_chunk",
        "live",
        json!({ "promptId": "turn-1" }),
    );
    let _ = apply_acp_notification(&mut projector, "session/update", &live)
        .expect("live 通知必须可投影");

    projector.begin_replay("session-1");
    let replay = text_notification(
        "agent_message_chunk",
        "history",
        json!({
            "eventId": "persisted-event-1",
            "promptId": "turn-0",
            "isReplay": true
        }),
    );
    let events = apply_acp_notification(&mut projector, "session/update", &replay)
        .expect("replay 通知必须可投影");

    assert_valid(&events);
    assert_eq!(events[0].sequence, 0);
    assert_eq!(events[0].event_id, "persisted-event-1");
    assert_eq!(events[0].origin, Origin::Replay);
    assert!(matches!(
        &events[0].block,
        KitBlock::Assistant { markdown, streaming } if markdown == "history" && !streaming
    ));
}

/// sidecar eventId 优先；缺失时使用冻结的 session/origin/sequence 回退值。
#[test]
fn uses_sidecar_event_id_before_deterministic_fallback() {
    let mut projector = Projector::new("scope-1");
    let preferred = text_notification(
        "user_message_chunk",
        "first",
        json!({ "eventId": "sidecar-wins", "promptId": "turn-1" }),
    );
    let fallback = text_notification(
        "user_message_chunk",
        "second",
        json!({ "promptId": "turn-2" }),
    );

    let preferred_events = apply_acp_notification(&mut projector, "session/update", &preferred)
        .expect("带 eventId 的通知必须可投影");
    let fallback_events = apply_acp_notification(&mut projector, "session/update", &fallback)
        .expect("无 eventId 的通知必须使用回退值");

    assert_valid(&preferred_events);
    assert_valid(&fallback_events);
    assert_eq!(preferred_events[0].event_id, "sidecar-wins");
    assert_eq!(fallback_events[0].event_id, "session-1:live:1");
}

/// 未知、plan、todo 以及 xAI session update 均不得让本批失败；live 必须发合成 Status。
#[test]
fn unknown_updates_are_counted_without_failing_live_projection() {
    let mut projector = Projector::new("scope-1");
    let updates = [
        json!({ "sessionUpdate": "future_update" }),
        json!({ "sessionUpdate": "plan", "entries": [] }),
        json!({ "sessionUpdate": "todo", "items": [] }),
    ];

    for update in updates {
        let notification = tool_notification(update, json!({}));
        let events = apply_acp_notification(&mut projector, "session/update", &notification)
            .expect("未知或禁用 update 不得让 live 批次报错");
        assert_valid(&events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].turn_id, None);
        assert_eq!(events[0].submission_id, None);
        assert_eq!(events[0].block_id, events[0].event_id);
        assert!(matches!(
            &events[0].block,
            KitBlock::Status { code, .. } if code == "skipped_update"
        ));
    }

    let xai_notification = tool_notification(
        json!({ "sessionUpdate": "rewind_marker", "target_prompt_index": 1 }),
        json!({}),
    );
    let events = apply_acp_notification(&mut projector, "_x.ai/session/update", &xai_notification)
        .expect("xAI session update 必须降级为 skipped_update，而不是错误");
    assert_valid(&events);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_id, "session-1:host:skipped_update:3");
    assert_eq!(events[0].block_id, events[0].event_id);
}

/// replay 中的未知 update 只累计，由后续 Task 7b 决定何时发出 replay_skipped。
#[test]
fn replay_unknown_updates_increment_a_count_without_emitting_a_status() {
    let mut projector = Projector::new("scope-1");
    projector.begin_replay("session-1");
    let replay_unknown = tool_notification(
        json!({ "sessionUpdate": "plan", "entries": [] }),
        json!({ "isReplay": true }),
    );

    let events = apply_acp_notification(&mut projector, "session/update", &replay_unknown)
        .expect("replay 未知 update 不得报错");

    assert!(events.is_empty(), "Task 6 不得提前发 replay_skipped");
    assert_eq!(projector.replay_skipped_count("session-1"), 1);
    assert_eq!(projector.take_replay_skipped_count("session-1"), 1);
    assert_eq!(projector.replay_skipped_count("session-1"), 0);
}

/// 无法归属到 session 的未知/禁用通知只能计数并安全忽略，不能把整批 ACP 流变成错误。
#[test]
fn unattributed_unknown_notifications_are_safe_noops() {
    let mut projector = Projector::new("scope-1");
    let notifications = [
        ("_x.ai/session/update", json!({})),
        ("session/update", json!({})),
        (
            "session/update",
            json!({ "update": { "sessionUpdate": "future_update" } }),
        ),
    ];

    for (method, params) in notifications {
        let events = apply_acp_notification(&mut projector, method, &params)
            .expect("无 sessionId 的未知或禁用更新必须安全忽略");
        assert!(events.is_empty(), "不可归属输入不得伪造 Kit 事件");
    }
    assert_eq!(
        projector.unattributed_skipped_count(),
        3,
        "每条无 sessionId 的未知/禁用通知都必须进入可观测计数"
    );
}

/// Projector 的调试输出只允许诊断计数，不能回显 assistant、thinking 或工具快照文本。
#[test]
fn projector_debug_redacts_projected_text_snapshots() {
    let mut assistant_projector = Projector::new("scope-1");
    let _ = apply_acp_notification(
        &mut assistant_projector,
        "session/update",
        &text_notification(
            "agent_message_chunk",
            "assistant-debug-secret",
            json!({ "promptId": "turn-1" }),
        ),
    )
    .expect("assistant 测试通知必须可投影");
    assert!(
        !format!("{assistant_projector:?}").contains("assistant-debug-secret"),
        "Projector Debug 不得回显 assistant 文本"
    );

    let mut thinking_projector = Projector::new("scope-1");
    let _ = apply_acp_notification(
        &mut thinking_projector,
        "session/update",
        &text_notification(
            "agent_thought_chunk",
            "thinking-debug-secret",
            json!({ "promptId": "turn-1" }),
        ),
    )
    .expect("thinking 测试通知必须可投影");
    assert!(
        !format!("{thinking_projector:?}").contains("thinking-debug-secret"),
        "Projector Debug 不得回显 thinking 文本"
    );

    let mut tool_projector = Projector::new("scope-1");
    let _ = apply_acp_notification(
        &mut tool_projector,
        "session/update",
        &tool_notification(
            json!({
                "sessionUpdate": "tool_call",
                "toolCallId": "tool-1",
                "title": "tool-name-debug-secret",
                "content": [{ "type": "text", "text": "tool-detail-debug-secret" }]
            }),
            json!({ "promptId": "turn-1" }),
        ),
    )
    .expect("tool 测试通知必须可投影");
    let debug = format!("{tool_projector:?}");
    assert!(
        !debug.contains("tool-name-debug-secret") && !debug.contains("tool-detail-debug-secret"),
        "Projector Debug 不得回显工具快照文本"
    );
}
