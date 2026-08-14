//! ACP stdio runtime 的集成测试。
//!
//! 每个用例都通过真实子进程 stdin/stdout 管道驱动 Runtime：子进程 stderr
//! 捕获 Host 写入，stdout 注入 sidecar 入站 JSON-RPC，避免用 mock 掩盖 wire 形状。

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, ChildStderr, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use efflab_agent_contract::HostPolicy;
use efflab_agent_host::{AcpRuntime, Inbound, METHOD_NOT_FOUND, RequestId, ValidatedReply};
use serde_json::{Value, json};

const POLL_TIMEOUT: Duration = Duration::from_secs(2);

/// 子进程只把收到的 Host stdin 行转发到 stderr，供测试断言实际 wire。
const CAPTURE_STDIN: &str = r#"
while IFS= read -r line; do
    printf '%s\n' "$line" >&2
done
"#;

/// 收到一个 Host request 后，在其 result 前插入 notification，验证读循环不会阻塞。
const INTERLEAVED_NOTIFICATION_AND_RESPONSE: &str = r#"
IFS= read -r line
printf '%s\n' "$line" >&2
printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"}}}}'
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"servers":[]}}'
while IFS= read -r line; do
    printf '%s\n' "$line" >&2
done
"#;

/// 先发标准 permission request，再发 ACP 扩展 request；两者都必须进 Inbound 队列。
const REVERSE_REQUESTS: &str = r#"
printf '%s\n' '{"jsonrpc":"2.0","id":17,"method":"session/request_permission","params":{"sessionId":"session-1","toolCall":{"toolCallId":"tool-1","title":"Run test"},"options":[{"optionId":"allow-once","name":"Allow once","kind":"allow_once"},{"optionId":"reject-once","name":"Reject once","kind":"reject_once"},{"optionId":"enable-always-approve","name":"Always allow","kind":"allow_once"}]}}'
printf '%s\n' '{"jsonrpc":"2.0","id":18,"method":"_x.ai/ask_user_question","params":{"questions":[]}}'
while IFS= read -r line; do
    printf '%s\n' "$line" >&2
done
"#;

/// 注入一个扩展 reverse request，模拟暂未支持的 sidecar 扩展。
const UNKNOWN_REVERSE_REQUEST: &str = r#"
printf '%s\n' '{"jsonrpc":"2.0","id":23,"method":"_x.ai/not_available","params":{}}'
while IFS= read -r line; do
    printf '%s\n' "$line" >&2
done
"#;

/// 包装真实的子进程管道；Host 连接 stdin/stdout，测试从 stderr 观察 Host 写入。
struct PipePeer {
    child: Child,
    stderr: BufReader<ChildStderr>,
}

/// 用一个最小 shell sidecar 构造拆分的 ACP stdio 端点。
fn runtime_with_peer(script: &str) -> (AcpRuntime, PipePeer) {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("测试 sidecar 必须启动");

    let stdin = child.stdin.take().expect("测试 sidecar 必须有 stdin");
    let stdout = child.stdout.take().expect("测试 sidecar 必须有 stdout");
    let stderr = child.stderr.take().expect("测试 sidecar 必须有 stderr");
    (
        AcpRuntime::new(stdin, stdout),
        PipePeer {
            child,
            stderr: BufReader::new(stderr),
        },
    )
}

impl PipePeer {
    /// 读取一条由测试 sidecar 捕获的 Host→sidecar JSON-RPC wire 消息。
    fn read_wire(&mut self) -> Value {
        let mut line = String::new();
        let size = self
            .stderr
            .read_line(&mut line)
            .expect("读取测试 sidecar stderr 必须成功");
        assert_ne!(size, 0, "预期 Host 写入一条 JSON-RPC 消息");
        serde_json::from_str(&line).expect("Host 写入必须是 JSON")
    }

    /// 在 Runtime 关闭 stdin 后回收子进程，并确认没有未断言的 Host 写入。
    fn finish(mut self) {
        let status = self.child.wait().expect("测试 sidecar 必须退出");
        assert!(status.success(), "测试 sidecar 退出状态异常: {status}");

        let mut remaining = String::new();
        self.stderr
            .read_to_string(&mut remaining)
            .expect("读取剩余 stderr 必须成功");
        assert!(remaining.is_empty(), "存在未断言的 Host wire: {remaining}");
    }
}

/// 等待后台 stdout 读循环投递一条入站消息，避免以阻塞 request 模拟 prompt 生命周期。
fn next_inbound(runtime: &AcpRuntime) -> Inbound {
    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        if let Some(inbound) = runtime
            .poll_inbound()
            .expect("读取 sidecar 入站消息必须成功")
        {
            return inbound;
        }
        assert!(Instant::now() < deadline, "等待 sidecar 入站消息超时");
        thread::sleep(Duration::from_millis(5));
    }
}

/// 构造本任务无需 cwd 校验的最小 Host contract 策略。
fn policy() -> HostPolicy {
    HostPolicy::new("/")
}

/// 违反 Host contract 的 initialize 必须在写入 stdin 前被拒绝。
#[test]
fn terminal_initialize_is_rejected_before_any_stdin_write() {
    let (runtime, peer) = runtime_with_peer(CAPTURE_STDIN);

    let error = runtime
        .request_validated(
            "initialize",
            json!({
                "protocolVersion": 1,
                "client": { "name": "test-host", "mcpServers": [] },
                "capabilities": { "terminal": true, "fs": false }
            }),
            &policy(),
        )
        .expect_err("terminal=true 必须在 Host contract 被拒绝");
    assert!(error.to_string().contains("terminal"));

    drop(runtime);
    peer.finish();
}

/// `session/cancel` 是 notification，wire 上绝不能分配 request id。
#[test]
fn cancel_notification_has_no_id_on_wire() {
    let (runtime, mut peer) = runtime_with_peer(CAPTURE_STDIN);

    runtime
        .notify_validated(
            "session/cancel",
            json!({ "sessionId": "session-1" }),
            &policy(),
        )
        .expect("合法 cancel notification 必须写入");

    let wire = peer.read_wire();
    assert_eq!(wire["jsonrpc"], "2.0");
    assert_eq!(wire["method"], "session/cancel");
    assert_eq!(wire["params"], json!({ "sessionId": "session-1" }));
    assert!(wire.get("id").is_none(), "notification 不得携带 id: {wire}");

    drop(runtime);
    peer.finish();
}

/// 扩展方法以逻辑名经过 contract 校验，但 ACP stdin 必须带 `_` wire 前缀。
#[test]
fn extension_request_uses_underscore_wire_prefix() {
    let (runtime, mut peer) = runtime_with_peer(CAPTURE_STDIN);

    let id = runtime
        .request_validated(
            "x.ai/mcp/list",
            json!({ "sessionId": "session-1" }),
            &policy(),
        )
        .expect("合法 x.ai/mcp/list 必须写入");

    let wire = peer.read_wire();
    assert_eq!(wire["jsonrpc"], "2.0");
    assert_eq!(wire["id"], json!(id.get()));
    assert_eq!(wire["method"], "_x.ai/mcp/list");
    assert_eq!(wire["params"], json!({ "sessionId": "session-1" }));

    drop(runtime);
    peer.finish();
}

/// request 返回后，通知和响应必须由独立读循环按 wire 顺序保留，而非被等待逻辑丢弃。
#[test]
fn inbound_notification_and_response_are_multiplexed() {
    let (runtime, mut peer) = runtime_with_peer(INTERLEAVED_NOTIFICATION_AND_RESPONSE);

    let id = runtime
        .request_validated(
            "x.ai/mcp/list",
            json!({ "sessionId": "session-1" }),
            &policy(),
        )
        .expect("request 必须立即返回 id，而非等待 response");
    assert_eq!(id, RequestId::new(1));

    let wire = peer.read_wire();
    assert_eq!(wire["method"], "_x.ai/mcp/list");

    let notification = next_inbound(&runtime);
    assert!(matches!(
        notification,
        Inbound::Notification { method, params }
            if method == "session/update" && params["sessionId"] == "session-1"
    ));

    let response = next_inbound(&runtime);
    assert!(matches!(
        response,
        Inbound::Response { id: response_id, result: Ok(result) }
            if response_id == id && result == json!({ "servers": [] })
    ));

    drop(runtime);
    peer.finish();
}

/// 标准 permission 和带 `_` 的 ACP 扩展 request 都必须解包，并只能回复本次列出的 option。
#[test]
fn reverse_requests_decode_and_permission_reply_uses_saved_options() {
    let (runtime, mut peer) = runtime_with_peer(REVERSE_REQUESTS);

    let permission_id = match next_inbound(&runtime) {
        Inbound::Request { id, method, params } => {
            assert_eq!(method, "session/request_permission");
            assert_eq!(params["options"][0]["optionId"], "allow-once");
            id
        }
        other => panic!("预期标准 permission reverse request，实际: {other:?}"),
    };

    let invalid = runtime.reply_validated(
        permission_id.clone(),
        ValidatedReply::Result(json!({
            "outcome": { "outcome": "selected", "optionId": "not-offered" }
        })),
        &policy(),
    );
    assert!(invalid.is_err(), "不在本次 options 的 optionId 必须被拒绝");

    runtime
        .reply_validated(
            permission_id.clone(),
            ValidatedReply::Result(json!({
                "outcome": { "outcome": "selected", "optionId": "allow-once" }
            })),
            &policy(),
        )
        .expect("本次 options 中的 allow-once 必须可回复");

    let reply_wire = peer.read_wire();
    assert_eq!(reply_wire["jsonrpc"], "2.0");
    assert_eq!(reply_wire["id"], json!(permission_id.get()));
    assert_eq!(
        reply_wire["result"],
        json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } })
    );
    assert!(reply_wire.get("method").is_none());

    match next_inbound(&runtime) {
        Inbound::Request { id, method, params } => {
            assert_eq!(id, RequestId::new(18));
            // ACP decoder 会去掉 extension wire method 的单个 `_` 前缀。
            assert_eq!(method, "x.ai/ask_user_question");
            assert_eq!(params, json!({ "questions": [] }));
        }
        other => panic!("预期 ACP extension reverse request，实际: {other:?}"),
    }

    drop(runtime);
    peer.finish();
}

/// 未知 reverse request 必须能以 JSON-RPC `method_not_found` error 回复，而非静默丢弃。
#[test]
fn unknown_reverse_request_can_receive_method_not_found_error() {
    let (runtime, mut peer) = runtime_with_peer(UNKNOWN_REVERSE_REQUEST);

    let id = match next_inbound(&runtime) {
        Inbound::Request { id, method, .. } => {
            assert_eq!(method, "x.ai/not_available");
            id
        }
        other => panic!("预期未知 extension reverse request，实际: {other:?}"),
    };

    runtime
        .reply_validated(
            id.clone(),
            ValidatedReply::Error {
                code: METHOD_NOT_FOUND,
                message: "Method not found".to_string(),
            },
            &policy(),
        )
        .expect("未知 reverse request 必须可发送 method_not_found error");

    let wire = peer.read_wire();
    assert_eq!(wire["jsonrpc"], "2.0");
    assert_eq!(wire["id"], json!(id.get()));
    assert_eq!(
        wire["error"],
        json!({ "code": METHOD_NOT_FOUND, "message": "Method not found" })
    );
    assert!(wire.get("result").is_none());

    drop(runtime);
    peer.finish();
}
