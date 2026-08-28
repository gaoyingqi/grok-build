use std::{future::Future, io::Write, path::PathBuf};

use agent_client_protocol as acp;
use async_trait::async_trait;
use futures::{future::LocalBoxFuture, io::Cursor};
use serde_json::value::RawValue;
use tokio::sync::oneshot;
use xai_acp_lib::{AcpAgentMessage, AcpArgs, AcpMethod, LineBufferedRead};

type ConnectionSpawn = fn(LocalBoxFuture<'static, ()>);

fn discard_spawn(_: LocalBoxFuture<'static, ()>) {}

/// 用具体参数 wrapper 调用真实构造函数，并返回 wrapper 的函数类型证据。
fn agent_side_connection_new_probe(
    agent: DummyAgent,
    outgoing: Cursor<Vec<u8>>,
    incoming: Cursor<Vec<u8>>,
    spawn: ConnectionSpawn,
) -> (
    acp::AgentSideConnection,
    impl Future<Output = acp::Result<()>>,
) {
    acp::AgentSideConnection::new(agent, outgoing, incoming, spawn)
}

/// 返回已编译 wrapper 的函数项类型名，不凭字符串手写构造函数签名。
fn agent_side_connection_new_type() -> String {
    let constructor: fn(DummyAgent, Cursor<Vec<u8>>, Cursor<Vec<u8>>, ConnectionSpawn) -> _ =
        agent_side_connection_new_probe;
    let _ = constructor(
        DummyAgent,
        Cursor::new(Vec::<u8>::new()),
        Cursor::new(Vec::<u8>::new()),
        discard_spawn,
    );
    std::any::type_name_of_val(&constructor).to_owned()
}

/// 用具体参数 wrapper 验证真实 spawn_local 输入约束。
fn line_buffered_read_spawn_local_probe(source: Cursor<Vec<u8>>) -> LineBufferedRead {
    LineBufferedRead::spawn_local(source)
}

/// 返回已编译 wrapper 的函数项类型名，不在同步测试中启动 Tokio 任务。
fn line_buffered_read_spawn_local_type() -> String {
    let spawn_local: fn(Cursor<Vec<u8>>) -> LineBufferedRead = line_buffered_read_spawn_local_probe;
    std::any::type_name_of_val(&spawn_local).to_owned()
}

/// 通过泛型 helper 逐一编译检查 ACP Agent trait 方法。
fn compile_check_agent_methods<A: acp::Agent>(agent: &A) {
    let _ = agent.initialize(acp::InitializeRequest::new(acp::ProtocolVersion::V1));
    let _ = agent.new_session(acp::NewSessionRequest::new("/tmp"));
    let _ = agent.load_session(acp::LoadSessionRequest::new("probe", "/tmp"));
    let _ = agent.prompt(acp::PromptRequest::new(
        acp::SessionId::new("probe"),
        Vec::new(),
    ));
    let _ = agent.cancel(acp::CancelNotification::new("probe"));
    let _ = agent.ext_method(ext_request());
    let _ = agent.list_sessions(acp::ListSessionsRequest::new());
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
        evidence.push(format!("Agent.{label}.name_constant={method}"));
    }
    compile_check_agent_methods(&DummyAgent);
    for label in [
        "initialize",
        "new_session",
        "load_session",
        "prompt",
        "cancel",
        "ext_method",
        "list_sessions",
    ] {
        evidence.push(format!("Agent.{label}.trait_method=compile_checked"));
    }

    let (response_tx, _response_rx) = oneshot::channel();
    let message = AcpAgentMessage::ListSessions(AcpArgs {
        request: acp::ListSessionsRequest::new().cursor("next"),
        response_tx,
    });
    assert_eq!(message.method_name(), acp::AGENT_METHOD_NAMES.session_list);
    assert_ne!(message.method_name(), "_x.ai/session/list");
    let serialized = serde_json::to_value(&message).expect("serialize session/list message");
    assert_eq!(serialized["method_name"], "session/list");
    assert_eq!(serialized["request"]["cursor"], "next");
    evidence.push("AcpAgentMessage.ListSessions=true".to_owned());
    evidence.push("AcpAgentMessage.ListSessions.method=session/list".to_owned());
    evidence.push("AcpAgentMessage.ListSessions.serialize=true".to_owned());

    // 该反序列化路径证明标准 session/list 会进入 ListSessions 变体。
    let decoded: AcpAgentMessage = serde_json::from_value(serde_json::json!({
        "method_name": "session/list",
        "request": { "cursor": "next" },
    }))
    .expect("deserialize session/list message");
    assert!(matches!(&decoded, AcpAgentMessage::ListSessions(_)));
    assert_eq!(decoded.method_name(), acp::AGENT_METHOD_NAMES.session_list);
    let boxed = decoded.boxed();
    assert_eq!(boxed.method_name(), acp::AGENT_METHOD_NAMES.session_list);
    evidence.push("AcpAgentMessage.ListSessions.deserialize=true".to_owned());
    evidence.push("AcpAgentMessage.ListSessions.boxed=true".to_owned());

    let constructor_type = agent_side_connection_new_type();
    evidence.push(format!(
        "AgentSideConnection::new=compile_checked;function_type={constructor_type}"
    ));
    let spawn_local_type = line_buffered_read_spawn_local_type();
    evidence.push(format!(
        "LineBufferedRead::spawn_local=compile_checked;function_type={spawn_local_type}"
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
