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

/// 先写半行，收到 Host 的同步 notification 后才补换行，锁定生产 reader 的分帧边界。
const PARTIAL_LINE_CHILD: &str = r#"
printf '%s' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"partial"'
printf '%s\n' 'half-line-ready' >&2
IFS= read -r control
printf '%s\n' "$control" >&2
printf '%s\n' '}}}}'
printf '%s\n' 'complete-line' >&2
while IFS= read -r line; do
    :
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

/// 包装 permission request 也必须保留 option 账本，不能因逻辑名变化跳过校验。
const WRAPPED_PERMISSION_REQUEST: &str = r#"
printf '%s\n' '{"jsonrpc":"2.0","id":19,"method":"_x.ai/session/request_permission","params":{"sessionId":"session-1","toolCall":{"toolCallId":"tool-1","title":"Run test"},"options":[{"optionId":"allow-once","name":"Allow once","kind":"allow_once"}]}}'
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

/// stdout 立即 EOF，用于确认传输终止不会被伪装成“暂无消息”。
const STDOUT_EOF: &str = "exit 0";

/// 攻击方持续输出没有换行的内容时，reader 必须在固定行上限处终止，而不能无限积累。
const OVERSIZED_UNTERMINATED_STDOUT: &str = r#"
head -c 1048577 /dev/zero | tr '\000' x
"#;

/// 同一个 reverse id 的第二个 request 带不同的 permission options，属于协议错误。
const DUPLICATE_REVERSE_REQUESTS: &str = r#"
printf '%s\n' '{"jsonrpc":"2.0","id":41,"method":"session/request_permission","params":{"options":[{"optionId":"first-option","name":"First","kind":"allow_once"}]}}'
IFS= read -r line
printf '%s\n' "$line" >&2
printf '%s\n' '{"jsonrpc":"2.0","id":41,"method":"session/request_permission","params":{"options":[{"optionId":"second-option","name":"Second","kind":"allow_once"}]}}'
while IFS= read -r line; do
    printf '%s\n' "$line" >&2
done
"#;

/// 非扩展 method 的前导 `_` 不能被 extension 映射误删。
const NON_EXTENSION_UNDERSCORE_REQUEST: &str = r#"
printf '%s\n' '{"jsonrpc":"2.0","id":24,"method":"_not_an_extension","params":{}}'
while IFS= read -r line; do
    printf '%s\n' "$line" >&2
done
"#;

/// 丢弃 Host 写入，供出站在途账本上限测试使用。
const DISCARD_STDIN: &str = r#"
while IFS= read -r line; do
    :
done
"#;

/// 每个 reverse request 必须等 Host 的确认后才发下一个，避免测试自身填满入站队列。
const MANY_REVERSE_REQUESTS: &str = r#"
i=1
while [ "$i" -le 65 ]; do
    printf '{"jsonrpc":"2.0","id":%s,"method":"_x.ai/test","params":{}}\n' "$i"
    IFS= read -r line
    printf '%s\n' "$line" >&2
    i=$((i + 1))
done
while IFS= read -r line; do
    :
done
"#;

/// 先灌满 notification 队列，再用 stdout 写失败作为 reader overflow 的握手信号。
const MANY_NOTIFICATIONS: &str = r#"
exec 3>&2
exec 2>/dev/null
trap '' PIPE
i=1
while [ "$i" -le 65 ]; do
    printf '{"jsonrpc":"2.0","method":"session/update","params":{"sequence":%s}}\n' "$i" 2>/dev/null
    i=$((i + 1))
done
while :; do
    if cat 2>/dev/null <<'EOF'
{"jsonrpc":"2.0","method":"queue-overflow-probe","params":{}}
EOF
    then
        sleep 0.01
    else
        printf '%s\n' 'queue-overflow-observed' >&3
        break
    fi
done
IFS= read -r line
printf '%s\n' 'stdin-closed-after-queue-overflow' >&3
"#;

/// 子进程退出后让孙进程暂时持有 stdout；Runtime Drop 必须先回收 reader，不能等该 fd 自行 EOF。
const STDOUT_HELD_BY_DESCENDANT: &str = r#"
(
    trap '' PIPE
    sleep 1
    if printf '%s\n' 'runtime-reader-still-open'; then
        printf '%s\n' 'reader-open' >&2
    else
        printf '%s\n' 'reader-closed' >&2
    fi
) < /dev/null &
while IFS= read -r line; do
    :
done
"#;

/// 包装真实的子进程管道；Host 连接 stdin/stdout，测试从 stderr 观察 Host 写入。
struct PipePeer {
    child: Child,
    stderr: BufReader<ChildStderr>,
}

/// 用一个最小 shell sidecar 构造拆分的 ACP stdio 端点。
fn runtime_with_peer(script: &str) -> (AcpRuntime, PipePeer) {
    #[allow(clippy::disallowed_methods)]
    // 集成测试 fixture：`PipePeer::finish` 在所有正常路径同步 wait 该短生命周期 shell。
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

    /// 读取 sidecar 的测试握手标记，不消费 Host→sidecar JSON wire。
    fn read_stderr_marker(&mut self) -> String {
        let mut line = String::new();
        let size = self
            .stderr
            .read_line(&mut line)
            .expect("读取测试 sidecar stderr 握手必须成功");
        assert_ne!(size, 0, "预期 sidecar stderr 握手标记");
        line.trim_end_matches(['\r', '\n']).to_string()
    }

    /// 在 Runtime 关闭 stdin 后回收子进程，并返回所有未逐行读取的 stderr 内容。
    fn finish_with_stderr(mut self) -> String {
        let status = self.child.wait().expect("测试 sidecar 必须退出");
        assert!(status.success(), "测试 sidecar 退出状态异常: {status}");

        let mut remaining = String::new();
        self.stderr
            .read_to_string(&mut remaining)
            .expect("读取剩余 stderr 必须成功");
        remaining
    }

    /// 在 Runtime 关闭 stdin 后回收子进程，并确认没有未断言的 Host 写入。
    fn finish(self) {
        let remaining = self.finish_with_stderr();
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
        thread::yield_now();
    }
}

/// 等待 reader 将传输终止或协议错误显式交给调用方，不能无限轮询 `None`。
fn next_inbound_error(runtime: &AcpRuntime) -> String {
    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        match runtime.poll_inbound() {
            Ok(Some(inbound)) => panic!("预期入站错误，实际收到消息: {inbound:?}"),
            Ok(None) => {
                assert!(Instant::now() < deadline, "等待 ACP 入站错误超时");
                thread::yield_now();
            }
            Err(error) => return error.to_string(),
        }
    }
}

/// 真实 AcpRuntime 必须等待 child 补齐换行后才投递一条完整 notification。
#[test]
fn production_reader_delivers_one_message_after_child_completes_partial_line() {
    let (runtime, mut peer) = runtime_with_peer(PARTIAL_LINE_CHILD);

    assert_eq!(peer.read_stderr_marker(), "half-line-ready");
    assert!(
        runtime
            .poll_inbound()
            .expect("补齐换行前读取 partial-line 必须成功")
            .is_none(),
        "child 只写出半行时，生产 AcpRuntime 不得投递入站消息"
    );
    runtime
        .notify_validated(
            "session/cancel",
            json!({ "sessionId": "session-1" }),
            &policy(),
        )
        .expect("Host 控制 notification 必须写入 child stdin");
    assert_eq!(peer.read_wire()["method"], "session/cancel");
    assert_eq!(peer.read_stderr_marker(), "complete-line");

    let inbound = next_inbound(&runtime);
    assert!(matches!(
        inbound,
        Inbound::Notification { method, params }
            if method == "session/update"
                && params["sessionId"] == "session-1"
                && params["update"]["content"]["text"] == "partial"
    ));
    assert!(
        runtime
            .poll_inbound()
            .expect("读取 partial-line 后的入站队列必须成功")
            .is_none(),
        "child 只补齐一行，Runtime 不得拆出或复制第二条消息"
    );

    drop(runtime);
    peer.finish();
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
                "clientCapabilities": {
                    "terminal": true,
                    "fs": { "readTextFile": false, "writeTextFile": false }
                },
                "clientInfo": { "name": "test-host", "version": "1.0.0" }
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

/// `_x.ai/session/request_permission` 解码后的逻辑名同样必须校验本次 options[]。
#[test]
fn wrapped_permission_reply_rejects_option_not_offered_by_the_saved_request() {
    let (runtime, peer) = runtime_with_peer(WRAPPED_PERMISSION_REQUEST);
    let permission_id = match next_inbound(&runtime) {
        Inbound::Request { id, method, params } => {
            assert_eq!(method, "x.ai/session/request_permission");
            assert_eq!(params["options"][0]["optionId"], "allow-once");
            id
        }
        other => panic!("预期包装 permission reverse request，实际: {other:?}"),
    };

    let invalid = runtime.reply_validated(
        permission_id,
        ValidatedReply::Result(json!({
            "outcome": { "outcome": "selected", "optionId": "not-offered" }
        })),
        &policy(),
    );
    assert!(
        invalid.is_err(),
        "包装 permission 也必须拒绝不属于保存 options[] 的 optionId"
    );

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

/// stdout EOF 必须变成可观察的终止错误，而不是让调用方无限得到 None。
#[test]
fn stdout_eof_is_reported_as_transport_error() {
    let (runtime, peer) = runtime_with_peer(STDOUT_EOF);

    let error = next_inbound_error(&runtime);
    assert!(
        error.contains("ACP stdout EOF"),
        "EOF 错误必须包含传输终止原因: {error}"
    );

    drop(runtime);
    peer.finish();
}

/// 不带换行的过长 stdout 帧必须在达到上限时终止 reader，避免 pending Vec 无界增长。
#[test]
fn unterminated_over_limit_stdout_frame_terminates_transport() {
    let (runtime, peer) = runtime_with_peer(OVERSIZED_UNTERMINATED_STDOUT);

    let error = next_inbound_error(&runtime);
    assert!(
        error.contains("行长度") || error.contains("line length"),
        "过长帧必须报告固定长度上限，而不是等待 EOF: {error}"
    );

    drop(runtime);
    peer.finish();
}

/// 同一个 reverse id 的重复 request 必须报错且保留第一次 options 账本。
#[test]
fn duplicate_reverse_id_preserves_original_permission_options() {
    let (runtime, mut peer) = runtime_with_peer(DUPLICATE_REVERSE_REQUESTS);

    let id = match next_inbound(&runtime) {
        Inbound::Request { id, method, params } => {
            assert_eq!(method, "session/request_permission");
            assert_eq!(params["options"][0]["optionId"], "first-option");
            id
        }
        other => panic!("预期第一次 permission reverse request，实际: {other:?}"),
    };

    // 给 sidecar 一个独立的 notification 作为继续发送重复 id 的同步点；不能回复
    // 第一次 request，否则重复 id 到达时原账本已经被消费。
    runtime
        .notify_validated(
            "session/cancel",
            json!({ "sessionId": "session-1" }),
            &policy(),
        )
        .expect("同步 notification 必须写入");
    let wire = peer.read_wire();
    assert_eq!(wire["method"], "session/cancel");
    assert!(wire.get("id").is_none());

    let duplicate_error = next_inbound_error(&runtime);
    assert!(
        duplicate_error.contains("重复") || duplicate_error.contains("duplicate"),
        "重复 reverse id 必须被明确拒绝: {duplicate_error}"
    );

    runtime
        .reply_validated(
            id.clone(),
            ValidatedReply::Result(json!({
                "outcome": { "outcome": "selected", "optionId": "first-option" }
            })),
            &policy(),
        )
        .expect("第一次 request 的原始 option 必须仍可回复");
    let reply_wire = peer.read_wire();
    assert_eq!(reply_wire["id"], json!(id.get()));
    assert_eq!(reply_wire["result"]["outcome"]["optionId"], "first-option");

    drop(runtime);
    peer.finish();
}

/// ACP decoder 只还原 `_x.ai/` 扩展前缀，非扩展 method 的前导 `_` 必须保留。
#[test]
fn non_extension_leading_underscore_is_preserved() {
    let (runtime, peer) = runtime_with_peer(NON_EXTENSION_UNDERSCORE_REQUEST);

    match next_inbound(&runtime) {
        Inbound::Request { method, .. } => assert_eq!(method, "_not_an_extension"),
        other => panic!("预期带前导下划线的 reverse request，实际: {other:?}"),
    }

    drop(runtime);
    peer.finish();
}

/// Host→sidecar 在途账本必须有上限，超限 request 在写入前 fail-closed。
#[test]
fn outbound_request_ledger_is_bounded_before_write() {
    let (runtime, peer) = runtime_with_peer(DISCARD_STDIN);
    let policy = policy();

    for _ in 0..64 {
        runtime
            .request_validated(
                "x.ai/mcp/list",
                json!({ "sessionId": "session-1" }),
                &policy,
            )
            .expect("达到上限前 request 必须可登记");
    }

    let error = runtime
        .request_validated(
            "x.ai/mcp/list",
            json!({ "sessionId": "session-1" }),
            &policy,
        )
        .expect_err("第 65 个在途 request 必须被拒绝");
    assert!(
        error.to_string().contains("上限") || error.to_string().contains("limit"),
        "超限错误必须可观察: {error}"
    );

    runtime.shutdown().expect("显式 shutdown 必须回收 runtime");
    peer.finish();
}

/// 超时调用方必须能仅按 request id 撤销自己的出站账本项，且未知 id 不得释放其它项。
#[test]
fn revoked_outbound_request_releases_only_its_ledger_slot() {
    let (runtime, peer) = runtime_with_peer(DISCARD_STDIN);
    let policy = policy();
    let mut request_ids = Vec::new();

    for _ in 0..64 {
        request_ids.push(
            runtime
                .request_validated(
                    "x.ai/mcp/list",
                    json!({ "sessionId": "session-1" }),
                    &policy,
                )
                .expect("达到上限前 request 必须可登记"),
        );
    }

    runtime
        .revoke_outbound_request(RequestId::new(10_000))
        .expect("撤销未知 request id 必须是幂等空操作");
    let error = runtime
        .request_validated(
            "x.ai/mcp/list",
            json!({ "sessionId": "session-1" }),
            &policy,
        )
        .expect_err("撤销未知 id 后账本仍必须保持满载");
    assert!(
        error.to_string().contains("上限") || error.to_string().contains("limit"),
        "未知 id 不得释放其它账本项: {error}"
    );

    runtime
        .revoke_outbound_request(request_ids[0])
        .expect("必须能按 request id 撤销超时的出站账本项");
    runtime
        .request_validated(
            "x.ai/mcp/list",
            json!({ "sessionId": "session-1" }),
            &policy,
        )
        .expect("撤销指定 request id 后必须能登记新的 request");

    runtime.shutdown().expect("显式 shutdown 必须回收 runtime");
    peer.finish();
}

/// session/cancel 只释放同一 session 的 pending request，释放后可以再次登记。
#[test]
fn cancel_releases_matching_outbound_requests() {
    let (runtime, peer) = runtime_with_peer(DISCARD_STDIN);
    let policy = policy();

    // 先占满账本：63 条属于将被取消的 session，1 条属于其它 session。
    for _ in 0..63 {
        runtime
            .request_validated(
                "x.ai/mcp/list",
                json!({ "sessionId": "session-1" }),
                &policy,
            )
            .expect("取消前 session-1 request 必须可登记");
    }
    runtime
        .request_validated(
            "x.ai/mcp/list",
            json!({ "sessionId": "session-2" }),
            &policy,
        )
        .expect("其它 session request 必须占用一个 pending 槽位");

    runtime
        .notify_validated(
            "session/cancel",
            json!({ "sessionId": "session-1" }),
            &policy,
        )
        .expect("session/cancel 必须写入后释放对应账本");

    // 如果 cancel 没有移除 session-1 记录，这里会因为 64 项上限失败。
    for _ in 0..63 {
        runtime
            .request_validated(
                "x.ai/mcp/list",
                json!({ "sessionId": "session-1" }),
                &policy,
            )
            .expect("cancel 后 session-1 必须可以再次登记 request");
    }
    let error = runtime
        .request_validated(
            "x.ai/mcp/list",
            json!({ "sessionId": "session-1" }),
            &policy,
        )
        .expect_err("未被取消的 session-2 pending 必须仍占用最后一个槽位");
    assert!(
        error.to_string().contains("上限") || error.to_string().contains("limit"),
        "取消后未匹配账本仍须受上限约束: {error}"
    );

    runtime.shutdown().expect("显式 shutdown 必须回收 runtime");
    peer.finish();
}

/// reverse request 账本达到上限时必须终止 reader，并清理全部 pending 记录。
#[test]
fn inbound_reverse_request_ledger_is_bounded_and_fails_closed() {
    let (runtime, mut peer) = runtime_with_peer(MANY_REVERSE_REQUESTS);
    let policy = policy();

    for expected_id in 1..=64 {
        match next_inbound(&runtime) {
            Inbound::Request { id, method, .. } => {
                assert_eq!(id, RequestId::new(expected_id));
                assert_eq!(method, "x.ai/test");
            }
            other => panic!("预期 reverse request {expected_id}，实际: {other:?}"),
        }

        runtime
            .notify_validated(
                "session/cancel",
                json!({ "sessionId": "session-1" }),
                &policy,
            )
            .expect("同步 notification 必须写入");
        let wire = peer.read_wire();
        assert_eq!(wire["method"], "session/cancel");
        assert!(wire.get("id").is_none());
    }

    let error = next_inbound_error(&runtime);
    assert!(
        error.contains("账本") || error.contains("limit"),
        "reverse request 账本超限必须可观察: {error}"
    );

    // 终止后账本已清理；旧 id 不得再被当作可回复 request。
    let reply_error = runtime
        .reply_validated(
            RequestId::new(1),
            ValidatedReply::Error {
                code: METHOD_NOT_FOUND,
                message: "Method not found".to_string(),
            },
            &policy,
        )
        .expect_err("transport 终止后 pending reverse request 必须清理");
    assert!(reply_error.to_string().contains("未找到"));

    drop(runtime);
    let remaining = peer.finish_with_stderr();
    assert!(
        remaining.trim().is_empty(),
        "存在未断言的 Host wire: {remaining:?}"
    );
}

/// 入站 notification 队列溢出必须报告错误，不能静默丢弃消息换取内存安全。
#[test]
fn inbound_notification_queue_overflow_is_observable() {
    let (runtime, mut peer) = runtime_with_peer(MANY_NOTIFICATIONS);

    // sidecar 只有在 reader 关闭 stdout（由队列 overflow 触发）后才发出握手，
    // 因此下面开始消费时 overflow 已确定发生，不依赖线程调度快慢。
    assert_eq!(peer.read_stderr_marker(), "queue-overflow-observed");

    for expected_sequence in 1..=64 {
        match next_inbound(&runtime) {
            Inbound::Notification { method, params } => {
                assert_eq!(method, "session/update");
                assert_eq!(params["sequence"], expected_sequence);
            }
            other => panic!("预期第 {expected_sequence} 条 notification，实际: {other:?}"),
        }
    }

    let error = next_inbound_error(&runtime);
    assert!(
        error.contains("队列") || error.contains("queue"),
        "入站队列超限必须可观察: {error}"
    );

    drop(runtime);
    let stderr = peer.finish_with_stderr();
    assert_eq!(stderr, "stdin-closed-after-queue-overflow\n");
}

/// Runtime Drop 必须关闭 reader 持有的 stdout fd，再回收 worker，不能留下阻塞线程。
#[test]
fn runtime_drop_closes_stdout_reader_before_worker_join() {
    let (runtime, peer) = runtime_with_peer(STDOUT_HELD_BY_DESCENDANT);

    drop(runtime);
    let stderr = peer.finish_with_stderr();
    assert!(
        stderr.contains("reader-closed"),
        "runtime Drop 后 stdout 读端必须关闭，实际 stderr: {stderr:?}"
    );
    assert!(
        !stderr.contains("reader-open"),
        "runtime Drop 不得遗留持有 stdout 的 reader worker: {stderr:?}"
    );
}
