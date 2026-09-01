//! Task 14 的 L3b Chat Completions 与 SSE client 合同测试。

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::mock_l3b::{MockL3b, MockResponse};
use efflab_agent_sidecar::model_client::{
    CancellationToken, HttpModelClient, ModelDelta, ModelError, ModelToolCall, ModelTurnRequest,
};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::time::timeout;

const MAX_REQUEST_BODY_BYTES: usize = 1_048_576;
const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_SSE_LINE_BYTES: usize = 65_536;

/// 构造不携带客户端模型覆盖字段的最小 turn 请求。
fn test_request() -> ModelTurnRequest {
    ModelTurnRequest::new(vec![json!({
        "role": "user",
        "content": "hello",
    })])
}

/// 创建使用测试 binding 的 HTTP client。
fn client_for(server: &MockL3b) -> HttpModelClient {
    HttpModelClient::for_test(server.loopback_url(), "bind-sentinel")
}

/// 读取一个 SSE delta，并断言它是文本。
async fn next_text(stream: &mut impl ModelStreamLike) -> String {
    match stream
        .recv()
        .await
        .expect("读取 SSE delta")
        .expect("应有 delta")
    {
        ModelDelta::Text(text) => text,
        other => panic!("预期文本 delta，实际为 {other:?}"),
    }
}

/// 为测试复用 client stream 的异步接收接口。
trait ModelStreamLike {
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<ModelDelta>, ModelError>> + '_;
}

impl ModelStreamLike for efflab_agent_sidecar::model_client::ModelStream {
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<ModelDelta>, ModelError>> + '_ {
        efflab_agent_sidecar::model_client::ModelStream::recv(self)
    }
}

#[tokio::test]
async fn streams_split_sse_deltas_with_binding_token_without_retry() {
    let server = MockL3b::sse_chunks(["hel", "lo", "[DONE]"]);
    let client = client_for(&server);
    let cancel = CancellationToken::new();
    let mut stream = client
        .stream_turn(test_request(), cancel)
        .await
        .expect("创建模型 SSE stream");

    assert_eq!(next_text(&mut stream).await, "hel");
    assert_eq!(next_text(&mut stream).await, "lo");
    assert_eq!(
        stream.recv().await.expect("读取 DONE"),
        Some(ModelDelta::Done)
    );
    assert_eq!(server.request_count(), 1, "一次 turn 不得自动重试");
    assert_eq!(server.authorization_values(), ["Bearer bind-sentinel"]);

    let request = server.received_request();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1/chat/completions");
    assert_eq!(request.headers["accept"], ["text/event-stream"]);
    assert_eq!(request.headers["content-type"], ["application/json"]);
    let received = server.received_json();
    assert_eq!(received["stream"], Value::Bool(true));
    assert_eq!(received["model"], "byok-user-model");
    let keys = received
        .as_object()
        .expect("请求 JSON object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            "messages".to_owned(),
            "model".to_owned(),
            "stream".to_owned()
        ]
    );
}

#[tokio::test]
async fn sends_only_closed_chat_completion_keys_with_tools() {
    let server = MockL3b::sse_chunks(["[DONE]"]);
    let request = test_request()
        .with_tools(vec![json!({
            "type": "function",
            "function": {
                "name": "lookup",
                "description": "lookup",
                "parameters": {"type": "object"}
            }
        })])
        .with_tool_choice(json!("auto"));
    let mut stream = client_for(&server)
        .stream_turn(request, CancellationToken::new())
        .await
        .expect("创建模型 SSE stream");
    assert_eq!(
        stream.recv().await.expect("读取 DONE"),
        Some(ModelDelta::Done)
    );

    let received = server.received_json();
    let keys = received
        .as_object()
        .expect("请求 JSON object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            "messages".to_owned(),
            "model".to_owned(),
            "stream".to_owned(),
            "tool_choice".to_owned(),
            "tools".to_owned(),
        ]
    );
    assert_eq!(received["tool_choice"], "auto");
    assert!(received["tools"].is_array());
}

#[tokio::test]
async fn rejects_non_stream_json_and_wrong_content_type() {
    for (content_type, body) in [
        (
            Some("application/json"),
            br#"{"choices":[{"message":{"content":"hello"}}]}"#.to_vec(),
        ),
        (Some("text/plain"), b"data: [DONE]\n\n".to_vec()),
        (None, b"data: [DONE]\n\n".to_vec()),
    ] {
        let server = MockL3b::new(MockResponse::body(200, content_type, body));
        let result = client_for(&server)
            .stream_turn(test_request(), CancellationToken::new())
            .await;
        assert!(
            matches!(result, Err(ModelError::InvalidResponse)),
            "{result:?}"
        );
    }
}

#[tokio::test]
async fn rejects_401_403_413_and_500_without_retry() {
    for status in [401, 403, 413, 500] {
        let server = MockL3b::new(MockResponse::body(status, Some("application/json"), b"{}"));
        let result = client_for(&server)
            .stream_turn(test_request(), CancellationToken::new())
            .await;
        assert!(
            matches!(result, Err(ModelError::Http { status: actual }) if actual == status),
            "status={status}, result={result:?}"
        );
        assert_eq!(server.request_count(), 1, "HTTP 错误不得触发重试");
    }
}

#[tokio::test]
async fn rejects_redirect_without_following_it() {
    let server = MockL3b::new(MockResponse::redirect("http://127.0.0.1:9/v1"));
    let result = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await;
    assert!(
        matches!(result, Err(ModelError::Http { status: 307 })),
        "redirect 必须 fail-closed: {result:?}"
    );
    assert_eq!(server.request_count(), 1, "redirect 不得发第二个请求");
}

#[tokio::test]
async fn rejects_oversized_request_before_sending_body() {
    let server = MockL3b::sse_chunks(["[DONE]"]);
    let oversized = "x".repeat(MAX_REQUEST_BODY_BYTES);
    let request = ModelTurnRequest::new(vec![json!({
        "role": "user",
        "content": oversized,
    })]);
    let result = client_for(&server)
        .stream_turn(request, CancellationToken::new())
        .await;
    assert!(matches!(result, Err(ModelError::ResponseTooLarge)));
    assert_eq!(server.request_count(), 0, "请求体超限不得触网");
}

#[tokio::test]
async fn rejects_oversized_sse_line() {
    let line = format!("data: {}\n\n", "x".repeat(MAX_SSE_LINE_BYTES + 1));
    let server = MockL3b::raw_sse_chunks([(line.into_bytes(), Duration::ZERO)]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("HTTP 200 应先返回 stream");
    assert!(matches!(
        stream.recv().await,
        Err(ModelError::ResponseTooLarge)
    ));
}

#[tokio::test]
async fn rejects_cumulative_oversized_sse_body() {
    let body = vec![b'x'; MAX_RESPONSE_BODY_BYTES + 1];
    let server = MockL3b::new(
        MockResponse::body(200, Some("text/event-stream"), body).without_content_length(),
    );
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("HTTP 200 应先返回 stream");
    assert!(matches!(
        stream.recv().await,
        Err(ModelError::ResponseTooLarge)
    ));
}

#[tokio::test]
async fn rejects_known_oversized_content_length_before_stream() {
    let server = MockL3b::new(
        MockResponse::body(200, Some("text/event-stream"), Vec::<u8>::new())
            .with_truncated_body(MAX_RESPONSE_BODY_BYTES + 1),
    );
    let result = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await;
    assert!(matches!(result, Err(ModelError::ResponseTooLarge)));
}

#[tokio::test]
async fn ignores_empty_sse_frames_and_non_data_lines() {
    let body = concat!(
        "event: ignored\n\n",
        "data:\n\n",
        "retry: 10\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let server = MockL3b::raw_sse_chunks([(body.as_bytes().to_vec(), Duration::ZERO)]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("创建模型 SSE stream");
    assert_eq!(next_text(&mut stream).await, "ok");
    assert_eq!(
        stream.recv().await.expect("读取 DONE"),
        Some(ModelDelta::Done)
    );
}

#[tokio::test]
async fn accepts_stop_finish_reason_but_waits_for_done() {
    let server = MockL3b::raw_sse_chunks([
        (
            b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n".to_vec(),
            Duration::ZERO,
        ),
        (b"data: [DONE]\n\n".to_vec(), Duration::from_millis(80)),
    ]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("创建模型 SSE stream");
    let started = std::time::Instant::now();
    assert_eq!(
        stream.recv().await.expect("读取 [DONE]"),
        Some(ModelDelta::Done)
    );
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "finish_reason 不能代替 [DONE]"
    );
}

#[tokio::test]
async fn accepts_tool_calls_finish_reason_but_waits_for_done() {
    let server = MockL3b::raw_sse_chunks([
        (
            b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n".to_vec(),
            Duration::ZERO,
        ),
        (b"data: [DONE]\n\n".to_vec(), Duration::from_millis(80)),
    ]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("创建模型 SSE stream");
    let started = std::time::Instant::now();
    assert_eq!(
        stream.recv().await.expect("读取 [DONE]"),
        Some(ModelDelta::Done)
    );
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "finish_reason 不能代替 [DONE]"
    );
}

#[tokio::test]
async fn rejects_invalid_sse_delta() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":5}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let server = MockL3b::raw_sse_chunks([(body.as_bytes().to_vec(), Duration::ZERO)]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("HTTP 200 应先返回 stream");
    assert!(matches!(
        stream.recv().await,
        Err(ModelError::InvalidResponse)
    ));
}

#[tokio::test]
async fn rejects_disconnect_before_done() {
    let body = b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n";
    let server = MockL3b::new(
        MockResponse::body(200, Some("text/event-stream"), body.to_vec()).with_truncated_body(4),
    );
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("HTTP 200 应先返回 stream");
    assert_eq!(next_text(&mut stream).await, "partial");
    assert!(matches!(stream.recv().await, Err(ModelError::Http { .. })));
}

#[tokio::test]
async fn aggregates_tool_call_fragments_before_done() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\"\"}}]}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"x\\\"}\"}}]}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let server = MockL3b::raw_sse_chunks([(body.as_bytes().to_vec(), Duration::ZERO)]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("创建模型 SSE stream");
    match stream
        .recv()
        .await
        .expect("读取工具调用")
        .expect("应有工具调用")
    {
        ModelDelta::ToolCall(call) => {
            assert_eq!(call.index, 0);
            assert_eq!(call.id.as_deref(), Some("call-1"));
            assert_eq!(call.name.as_deref(), Some("lookup"));
            assert_eq!(call.arguments, "{\"q\":\"x\"}");
        }
        other => panic!("预期工具调用，实际为 {other:?}"),
    }
    assert_eq!(
        stream.recv().await.expect("读取 DONE"),
        Some(ModelDelta::Done)
    );
}

#[tokio::test]
async fn aggregates_multiple_tool_calls_by_explicit_index() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"lookup_one\",\"arguments\":\"{\\\"one\\\":1}\"}},{\"index\":0,\"id\":\"call-0\",\"type\":\"function\",\"function\":{\"name\":\"lookup_zero\",\"arguments\":\"{}\"}}]}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let server = MockL3b::raw_sse_chunks([(body.as_bytes().to_vec(), Duration::ZERO)]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("创建模型 SSE stream");

    match stream.recv().await.expect("读取第一个工具调用") {
        Some(ModelDelta::ToolCall(call)) => {
            assert_eq!(call.index, 0);
            assert_eq!(call.id.as_deref(), Some("call-0"));
            assert_eq!(call.name.as_deref(), Some("lookup_zero"));
            assert_eq!(call.arguments, "{}");
        }
        other => panic!("预期 index=0 工具调用，实际为 {other:?}"),
    }
    match stream.recv().await.expect("读取第二个工具调用") {
        Some(ModelDelta::ToolCall(call)) => {
            assert_eq!(call.index, 1);
            assert_eq!(call.id.as_deref(), Some("call-1"));
            assert_eq!(call.name.as_deref(), Some("lookup_one"));
            assert_eq!(call.arguments, "{\"one\":1}");
        }
        other => panic!("预期 index=1 工具调用，实际为 {other:?}"),
    }
    assert_eq!(
        stream.recv().await.expect("读取 DONE"),
        Some(ModelDelta::Done)
    );
}

#[tokio::test]
async fn rejects_tool_call_without_index() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let server = MockL3b::raw_sse_chunks([(body.as_bytes().to_vec(), Duration::ZERO)]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("HTTP 200 应先返回 stream");
    assert!(matches!(
        stream.recv().await,
        Err(ModelError::InvalidResponse)
    ));
}

#[tokio::test]
async fn rejects_conflicting_tool_call_fragments() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\"}}]}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-2\",\"type\":\"function\",\"function\":{\"arguments\":\"}\"}}]}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let server = MockL3b::raw_sse_chunks([(body.as_bytes().to_vec(), Duration::ZERO)]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("HTTP 200 应先返回 stream");
    assert!(matches!(
        stream.recv().await,
        Err(ModelError::InvalidResponse)
    ));
}

#[tokio::test]
async fn rejects_non_function_tool_call_type() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"custom\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let server = MockL3b::raw_sse_chunks([(body.as_bytes().to_vec(), Duration::ZERO)]);
    let mut stream = client_for(&server)
        .stream_turn(test_request(), CancellationToken::new())
        .await
        .expect("HTTP 200 应先返回 stream");
    assert!(matches!(
        stream.recv().await,
        Err(ModelError::InvalidResponse)
    ));
}

#[tokio::test]
async fn rejects_incomplete_tool_call_at_done() {
    for body in [
        concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]}}]}\n\n",
            "data: [DONE]\n\n",
        ),
        concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"arguments\":\"{}\"}}]}}]}\n\n",
            "data: [DONE]\n\n",
        ),
    ] {
        let server = MockL3b::raw_sse_chunks([(body.as_bytes().to_vec(), Duration::ZERO)]);
        let mut stream = client_for(&server)
            .stream_turn(test_request(), CancellationToken::new())
            .await
            .expect("HTTP 200 应先返回 stream");
        assert!(matches!(
            stream.recv().await,
            Err(ModelError::InvalidResponse)
        ));
    }
}

#[tokio::test]
async fn cancellation_stops_before_a_late_sse_chunk() {
    let server = MockL3b::sse_chunks_with_delays([
        ("first", Duration::ZERO),
        ("late", Duration::from_millis(250)),
        ("[DONE]", Duration::ZERO),
    ]);
    let token = CancellationToken::new();
    let mut stream = client_for(&server)
        .stream_turn(test_request(), token.clone())
        .await
        .expect("创建模型 SSE stream");
    assert_eq!(next_text(&mut stream).await, "first");
    let waiting_for_late_chunk = stream.recv();
    tokio::time::sleep(Duration::from_millis(20)).await;
    token.cancel();
    assert!(matches!(
        waiting_for_late_chunk.await,
        Err(ModelError::Cancelled)
    ));
    assert_eq!(stream.recv().await.expect("取消后只完成一次"), None);
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn cancellation_before_send_does_not_reach_mock() {
    let server = MockL3b::sse_chunks(["[DONE]"]);
    let token = CancellationToken::new();
    token.cancel();
    let result = client_for(&server).stream_turn(test_request(), token).await;
    assert!(matches!(result, Err(ModelError::Cancelled)));
    assert_eq!(server.request_count(), 0, "取消前不得创建 HTTP 请求");
}

#[tokio::test]
async fn cancellation_interrupts_initial_response_headers() {
    let gate = Arc::new(Notify::new());
    let server = MockL3b::new(
        MockResponse::sse_chunks(["[DONE]"]).with_response_header_gate(Arc::clone(&gate)),
    );
    let client = client_for(&server);
    let token = CancellationToken::new();
    let task_token = token.clone();
    let mut task =
        tokio::spawn(async move { client.stream_turn(test_request(), task_token).await });

    timeout(Duration::from_secs(1), server.wait_for_request())
        .await
        .expect("mock 应捕获初始请求");
    token.cancel();
    let result = timeout(Duration::from_millis(500), &mut task).await;

    // 仅在超时仍挂起时收尾，避免重复 poll 已完成的 JoinHandle。
    if result.is_err() {
        gate.notify_one();
        let _ = timeout(Duration::from_secs(1), &mut task).await;
    } else {
        gate.notify_one();
    }

    assert!(
        matches!(result, Ok(Ok(Err(ModelError::Cancelled)))),
        "初始 response headers 等待必须可取消: {result:?}"
    );
}

#[tokio::test]
async fn debug_views_redact_prompt_binding_and_tool_arguments() {
    let prompt_secret = "prompt-secret-for-debug";
    let binding_secret = "binding-secret-for-debug";
    let tool_name_secret = "tool-name-secret-for-debug";
    let tool_args_secret = "tool-args-secret-for-debug";
    let request = ModelTurnRequest::new(vec![json!({
        "role": "user",
        "content": prompt_secret,
    })])
    .with_tools(vec![json!({
        "type": "function",
        "function": {
            "name": tool_name_secret,
            "description": tool_args_secret,
            "parameters": {"type": "object"}
        }
    })]);
    let request_debug = format!("{request:?}");
    let delta_debug = format!(
        "{:?}",
        ModelDelta::ToolCall(ModelToolCall {
            index: 0,
            id: Some(binding_secret.to_owned()),
            name: Some(tool_name_secret.to_owned()),
            arguments: tool_args_secret.to_owned(),
        })
    );

    let server = MockL3b::sse_chunks(["[DONE]"]);
    let mut stream = HttpModelClient::for_test(server.loopback_url(), binding_secret)
        .stream_turn(request, CancellationToken::new())
        .await
        .expect("创建模型 SSE stream");
    let stream_debug = format!("{stream:?}");

    for debug in [request_debug, delta_debug, stream_debug] {
        assert!(!debug.contains(prompt_secret), "Debug 泄漏 prompt: {debug}");
        assert!(
            !debug.contains(binding_secret),
            "Debug 泄漏 binding: {debug}"
        );
        assert!(
            !debug.contains(tool_name_secret),
            "Debug 泄漏 tool name: {debug}"
        );
        assert!(
            !debug.contains(tool_args_secret),
            "Debug 泄漏 tool args: {debug}"
        );
    }
    assert_eq!(
        stream.recv().await.expect("读取 DONE"),
        Some(ModelDelta::Done)
    );
}

#[tokio::test]
async fn rejects_invalid_base_urls() {
    assert!(HttpModelClient::try_for_test("https://user.example/v1", "bind").is_err());
    assert!(HttpModelClient::try_for_test("http://localhost:4312/v1", "bind").is_err());
    assert!(HttpModelClient::try_for_test("http://127.0.0.2:4312/v1", "bind").is_err());
    assert!(HttpModelClient::try_for_test("http://127.0.0.1:4312/v1?x=1", "bind").is_err());
}
