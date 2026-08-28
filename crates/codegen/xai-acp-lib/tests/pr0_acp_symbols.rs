use std::{io::Write, path::PathBuf};

use agent_client_protocol as acp;
use async_trait::async_trait;
use futures::io::Cursor;
use serde_json::value::RawValue;
use tokio::sync::oneshot;
use xai_acp_lib::{AcpAgentMessage, AcpArgs, AcpMethod, LineBufferedRead};

/// 仅用于编译期验证 AgentSideConnection::new 的真实参数约束。
#[allow(dead_code)]
fn assert_agent_side_connection_new_signature() {
    let _ = acp::AgentSideConnection::new(
        DummyAgent,
        Cursor::new(Vec::<u8>::new()),
        Cursor::new(Vec::<u8>::new()),
        |_future| {},
    );
}

/// 仅用于编译期验证 LineBufferedRead::spawn_local 的真实输入约束。
#[allow(dead_code)]
fn assert_line_buffered_read_spawn_local_signature() {
    let _ = LineBufferedRead::spawn_local(Cursor::new(Vec::<u8>::new()));
}

/// 通过最小 Agent 实现让 Rust 校验 ACP 的标准 Agent 方法面。
struct DummyAgent;

#[async_trait(?Send)]
impl acp::Agent for DummyAgent {
    async fn initialize(
        &self,
        _args: acp::InitializeRequest,
    ) -> acp::Result<acp::InitializeResponse> {
        unreachable!()
    }

    async fn authenticate(
        &self,
        _args: acp::AuthenticateRequest,
    ) -> acp::Result<acp::AuthenticateResponse> {
        unreachable!()
    }

    async fn new_session(
        &self,
        _args: acp::NewSessionRequest,
    ) -> acp::Result<acp::NewSessionResponse> {
        unreachable!()
    }

    async fn prompt(&self, _args: acp::PromptRequest) -> acp::Result<acp::PromptResponse> {
        unreachable!()
    }

    async fn cancel(&self, _args: acp::CancelNotification) -> acp::Result<()> {
        unreachable!()
    }

    // 直接实现标准 list_sessions，避免把方法存在性误判为扩展方法。
    async fn list_sessions(
        &self,
        _args: acp::ListSessionsRequest,
    ) -> acp::Result<acp::ListSessionsResponse> {
        Ok(acp::ListSessionsResponse::new(Vec::new()))
    }
}

fn ext_request() -> acp::ExtRequest {
    let raw = RawValue::from_string(r#"{"request":"value"}"#.to_owned()).expect("raw request");
    acp::ExtRequest::new("probe/ext", raw.into())
}

fn ext_response() -> acp::ExtResponse {
    let raw = RawValue::from_string(r#"{"response":"value"}"#.to_owned()).expect("raw response");
    acp::ExtResponse::new(raw.into())
}

fn object_keys(value: &serde_json::Value) -> Vec<String> {
    value
        .as_object()
        .expect("serialized extension value must be an object")
        .keys()
        .cloned()
        .collect()
}

/// 输出本轮最小 sidecar 所依赖的 ACP 符号证据，并验证标准 session/list 分发入口。
#[test]
fn prints_acp_symbols_required_by_minimal_sidecar() {
    let methods = [
        ("initialize", acp::AGENT_METHOD_NAMES.initialize),
        ("new_session", acp::AGENT_METHOD_NAMES.session_new),
        ("load_session", acp::AGENT_METHOD_NAMES.session_load),
        ("prompt", acp::AGENT_METHOD_NAMES.session_prompt),
        ("cancel", acp::AGENT_METHOD_NAMES.session_cancel),
        ("ext_method", "ext_method"),
        ("list_sessions", acp::AGENT_METHOD_NAMES.session_list),
    ];
    let mut evidence = vec!["requires_unstable=true".to_owned()];
    for (label, method) in methods {
        assert!(!method.is_empty(), "Agent 缺少方法 {label}");
        evidence.push(format!("Agent.{label}={method}"));
    }

    let (response_tx, _response_rx) = oneshot::channel();
    let message = AcpAgentMessage::ListSessions(AcpArgs {
        request: acp::ListSessionsRequest::new(),
        response_tx,
    });
    assert_eq!(message.method_name(), acp::AGENT_METHOD_NAMES.session_list);
    assert_ne!(message.method_name(), "_x.ai/session/list");
    let serialized = serde_json::to_value(&message).expect("serialize session/list message");
    assert_eq!(serialized["method_name"], "session/list");
    evidence.push("AcpAgentMessage.ListSessions=true".to_owned());
    evidence.push("AcpAgentMessage.ListSessions.method=session/list".to_owned());
    evidence.push("AcpAgentMessage.ListSessions.serialize=true".to_owned());

    // 该反序列化路径证明标准 session/list 会进入 ListSessions 变体。
    let decoded: AcpAgentMessage = serde_json::from_value(serde_json::json!({
        "method_name": "session/list",
        "request": { "cursor": "next" },
    }))
    .expect("deserialize session/list message");
    assert_eq!(decoded.method_name(), acp::AGENT_METHOD_NAMES.session_list);
    let boxed = decoded.boxed();
    assert_eq!(boxed.method_name(), acp::AGENT_METHOD_NAMES.session_list);
    evidence.push("AcpAgentMessage.ListSessions.deserialize=true".to_owned());
    evidence.push("AcpAgentMessage.ListSessions.boxed=true".to_owned());

    evidence.push(format!(
        "AgentSideConnection::new=compile_checked;type={}",
        std::any::type_name::<acp::AgentSideConnection>()
    ));
    evidence.push(format!(
        "LineBufferedRead::spawn_local=compile_checked;type={}",
        std::any::type_name::<fn(Cursor<Vec<u8>>) -> LineBufferedRead>()
    ));

    let ext_req = serde_json::to_value(ext_request()).expect("serialize ext request");
    let ext_resp = serde_json::to_value(ext_response()).expect("serialize ext response");
    evidence.push(format!("ExtRequest.fields={:?}", object_keys(&ext_req)));
    evidence.push(format!("ExtResponse.fields={:?}", object_keys(&ext_resp)));

    let evidence = evidence.join("\n") + "\n";
    eprint!("{evidence}");
    write_symbol_log(&evidence);
}

/// 验证 ListSessions 请求会调用 Agent 的标准方法，而不是扩展方法。
#[tokio::test]
async fn list_sessions_message_routes_to_agent() {
    let (response_tx, response_rx) = oneshot::channel();
    let message = AcpAgentMessage::ListSessions(AcpArgs {
        request: acp::ListSessionsRequest::new(),
        response_tx,
    });
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            message.route_to_agent(DummyAgent, |future| {
                tokio::task::spawn_local(future);
            });
            let response = response_rx.await.expect("list_sessions response");
            let response = response.expect("list_sessions succeeded");
            assert!(response.sessions.is_empty());
        })
        .await;
}

fn write_symbol_log(evidence: &str) {
    let dir = std::env::temp_dir().join("efflab-sidecar-pr0");
    std::fs::create_dir_all(&dir).expect("create probe log directory");
    let path: PathBuf = dir.join("acp-symbols.txt");
    let mut file = std::fs::File::create(path).expect("create probe log");
    file.write_all(evidence.as_bytes())
        .expect("write probe log");
}
