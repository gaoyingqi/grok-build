//! Task 15 v1 与 Task 16 legacy session store 合同测试。

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use efflab_agent_sidecar::session_store::{
    MAX_JSON_DEPTH, MAX_LINE_BYTES, MAX_RECORDS, MAX_SESSION_FILE_BYTES, SessionError,
    SessionRecord, SessionRepository,
};
use tempfile::{TempDir, tempdir};

struct TestStore {
    _temporary: TempDir,
    home: PathBuf,
    repository: SessionRepository,
}

fn test_store() -> TestStore {
    let temporary = tempdir().expect("创建 session store 测试目录");
    let home = temporary.path().join("home");
    std::fs::create_dir(&home).expect("创建 session store home");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o700))
            .expect("设置 session store home 私有权限");
    }
    let repository = SessionRepository::new(home.clone());
    TestStore {
        _temporary: temporary,
        home,
        repository,
    }
}

fn session_dir(test: &TestStore, session_id: &str) -> PathBuf {
    test.home
        .join("efflab-sessions")
        .join("v1")
        .join(session_id)
}

fn manifest_path(test: &TestStore, session_id: &str) -> PathBuf {
    session_dir(test, session_id).join("manifest.json")
}

fn records_path(test: &TestStore, session_id: &str) -> PathBuf {
    session_dir(test, session_id).join("records.jsonl")
}

fn legacy_session_dir(test: &TestStore, fixture: &str) -> PathBuf {
    let destination = test
        .home
        .join("sessions")
        .join("%2Flegacy%2Fworkspace")
        .join("legacy-1");
    copy_fixture_dir(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy")
            .join(fixture),
        &destination,
    );
    destination
}

fn test_store_with_legacy_dir(fixture: &str) -> TestStore {
    let test = test_store();
    legacy_session_dir(&test, fixture);
    test
}

fn copy_fixture_dir(source: PathBuf, destination: &Path) {
    std::fs::create_dir_all(destination).expect("创建 legacy fixture 目录");
    for entry in std::fs::read_dir(source).expect("读取 legacy fixture") {
        let entry = entry.expect("读取 legacy fixture entry");
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_fixture_dir(source_path, &destination_path);
        } else {
            std::fs::copy(source_path, destination_path).expect("复制 legacy fixture 文件");
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o700))
            .expect("设置 legacy fixture 目录权限");
    }
}

fn legacy_fingerprint(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = std::fs::read_dir(root)
        .expect("读取 legacy session 目录")
        .map(|entry| {
            let entry = entry.expect("读取 legacy session entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            let bytes = std::fs::read(entry.path()).expect("读取 legacy session 文件");
            (name, bytes)
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

async fn ensure_prompt_allowed(
    repository: &SessionRepository,
    session_id: &str,
) -> Result<(), SessionError> {
    repository.ensure_prompt_allowed(session_id).await
}

fn text_of(record: &SessionRecord) -> Option<&str> {
    match record {
        SessionRecord::User { text, .. } | SessionRecord::AssistantSnapshot { text, .. } => {
            Some(text)
        }
        _ => None,
    }
}

fn has_terminal(record: &SessionRecord) -> bool {
    matches!(record, SessionRecord::TurnTerminal { .. })
}

fn has_tool(record: &SessionRecord) -> bool {
    matches!(record, SessionRecord::Tool { .. })
}

fn user_record(prompt_id: &str, sequence: u64) -> SessionRecord {
    SessionRecord::User {
        sequence,
        prompt_id: prompt_id.to_owned(),
        text: "hello".to_owned(),
    }
}

fn assistant_record(prompt_id: &str, sequence: u64) -> SessionRecord {
    SessionRecord::AssistantSnapshot {
        sequence,
        prompt_id: prompt_id.to_owned(),
        block_id: "block-a".to_owned(),
        text: "world".to_owned(),
        streaming: false,
    }
}

fn assert_corrupt(error: SessionError) {
    assert!(
        matches!(error, SessionError::Corrupt),
        "实际错误: {error:?}"
    );
}

#[tokio::test]
async fn v1_session_round_trip_preserves_prompt_id_and_record_sequence() {
    let test = test_store();
    let session = test.repository.create().await.unwrap();
    test.repository
        .append(
            &session.id,
            &[user_record("prompt-a", 0), assistant_record("prompt-a", 1)],
        )
        .await
        .unwrap();

    let loaded = test.repository.load(&session.id).await.unwrap();
    assert_eq!(loaded.id, session.id);
    assert_eq!(loaded.records[0].prompt_id(), Some("prompt-a"));
    assert_eq!(loaded.records[1].sequence(), 1);
}

#[tokio::test]
async fn v1_invalid_qualified_tool_is_preserved_as_an_audit_value() {
    let test = test_store();
    let session = test
        .repository
        .create_with_id("v1-invalid-tool")
        .await
        .unwrap();
    let invalid_name = "fixture__bad.name";
    test.repository
        .append(
            &session.id,
            &[
                user_record("prompt-v1", 0),
                SessionRecord::assistant_tool_calls(
                    1,
                    "prompt-v1",
                    0,
                    [("tool-1".to_owned(), invalid_name.to_owned())],
                    "",
                ),
                SessionRecord::tool_in_round(
                    2,
                    "prompt-v1",
                    0,
                    "tool-1",
                    invalid_name,
                    "historical tool",
                    "completed",
                ),
                SessionRecord::turn_terminal(3, "prompt-v1", "completed"),
            ],
        )
        .await
        .unwrap();

    // v1 journal 保留合法 identifier 形式的历史值；安全模型入口另由 transcript gate 过滤。
    let expected = BTreeSet::from([invalid_name.to_owned()]);
    let loaded = test
        .repository
        .load_with_tool_policy(&session.id, &expected, &expected)
        .await
        .unwrap();
    assert!(
        !loaded.read_only,
        "v1 审计记录不应因历史工具名被静默改成 legacy 只读"
    );
    assert!(loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::AssistantToolCalls { tool_calls, .. }
            if tool_calls.iter().any(|call| call.name == invalid_name))
    }));
    assert!(loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::Tool { name, .. } if name == invalid_name)
    }));
}

#[tokio::test]
async fn create_and_list_use_v1_directories_and_stable_order() {
    let test = test_store();
    test.repository.create_with_id("z-session").await.unwrap();
    test.repository.create_with_id("a-session").await.unwrap();

    let listed = test.repository.list().await.unwrap();
    let ids = listed
        .into_iter()
        .map(|summary| summary.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, ["a-session", "z-session"]);
    assert!(manifest_path(&test, "a-session").is_file());
    assert!(records_path(&test, "a-session").is_file());
}

#[tokio::test]
async fn torn_manifest_is_fail_closed_without_echoing_path_or_contents() {
    let test = test_store();
    let session = test.repository.create_with_id("session-1").await.unwrap();
    let secret = "manifest-secret-sentinel";
    std::fs::write(
        manifest_path(&test, &session.id),
        format!(r#"{{"schema_version":1,"session_id":"{secret}""#),
    )
    .expect("写入 torn manifest");

    let error = test.repository.load(&session.id).await.unwrap_err();
    assert!(
        matches!(&error, SessionError::Corrupt),
        "实际错误: {error:?}"
    );
    let rendered = error.to_string();
    assert!(!rendered.contains(secret));
    assert!(!rendered.contains(test.home.to_str().unwrap()));
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_manifest_is_fail_closed() {
    use std::os::unix::fs::symlink;

    let test = test_store();
    let session = test.repository.create_with_id("symlinked").await.unwrap();
    let target = test._temporary.path().join("manifest-target.json");
    std::fs::write(&target, br#"{"schema_version":1,"session_id":"symlinked"}"#)
        .expect("写入符号链接目标");
    std::fs::remove_file(manifest_path(&test, &session.id)).expect("删除原 manifest");
    symlink(&target, manifest_path(&test, &session.id)).expect("创建 manifest 符号链接");

    let error = test.repository.load(&session.id).await.unwrap_err();
    assert_corrupt(error);
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_home_ancestor_is_fail_closed() {
    use std::os::unix::fs::symlink;

    let test = test_store();
    let session = test
        .repository
        .create_with_id("ancestor-link")
        .await
        .unwrap();
    let alias = test._temporary.path().join("home-alias");
    symlink(test._temporary.path(), &alias).expect("创建 home 祖先符号链接");
    let aliased_repository = SessionRepository::new(alias.join("home"));

    let error = aliased_repository.load(&session.id).await.unwrap_err();
    assert_corrupt(error);
}

#[tokio::test]
async fn invalid_session_ids_are_rejected_before_path_join() {
    let test = test_store();
    let long_id = "a".repeat(129);
    for id in ["", ".", "..", "../escape", "a/b", "a\\b", "é", &long_id] {
        let error = test
            .repository
            .create_with_id(id)
            .await
            .expect_err("非法 session id 必须拒绝");
        assert!(matches!(error, SessionError::InvalidRecord), "id={id:?}");
    }
    assert!(!test.home.join("escape").exists());
}

#[tokio::test]
async fn oversized_manifest_is_rejected_at_the_file_limit() {
    let test = test_store();
    let session = test
        .repository
        .create_with_id("large-manifest")
        .await
        .unwrap();
    std::fs::write(
        manifest_path(&test, &session.id),
        vec![b'x'; MAX_SESSION_FILE_BYTES + 1],
    )
    .expect("写入超大 manifest");

    let error = test.repository.load(&session.id).await.unwrap_err();
    assert_corrupt(error);
}

#[tokio::test]
async fn oversized_records_file_is_rejected_at_the_file_limit() {
    let test = test_store();
    let session = test
        .repository
        .create_with_id("large-records")
        .await
        .unwrap();
    std::fs::write(
        records_path(&test, &session.id),
        vec![b'x'; MAX_SESSION_FILE_BYTES + 1],
    )
    .expect("写入超大 records 文件");

    let error = test.repository.load(&session.id).await.unwrap_err();
    assert_corrupt(error);
}

#[tokio::test]
async fn oversized_record_line_is_rejected_before_json_decode() {
    let test = test_store();
    let session = test.repository.create_with_id("large-line").await.unwrap();
    let mut line = vec![b'x'; MAX_LINE_BYTES + 1];
    line.push(b'\n');
    std::fs::write(records_path(&test, &session.id), line).expect("写入超大 records 行");

    let error = test.repository.load(&session.id).await.unwrap_err();
    assert_corrupt(error);
}

#[tokio::test]
async fn record_count_limit_is_enforced_without_partial_append() {
    let test = test_store();
    let session = test
        .repository
        .create_with_id("record-limit")
        .await
        .unwrap();
    let records = (0..MAX_RECORDS)
        .map(|sequence| user_record("limit-prompt", sequence as u64))
        .collect::<Vec<_>>();
    test.repository.append(&session.id, &records).await.unwrap();
    let before = std::fs::read(records_path(&test, &session.id)).expect("读取 records");

    let error = test
        .repository
        .append(
            &session.id,
            &[user_record("limit-prompt", MAX_RECORDS as u64)],
        )
        .await
        .unwrap_err();
    assert!(matches!(error, SessionError::InvalidRecord));
    assert_eq!(
        std::fs::read(records_path(&test, &session.id)).expect("再次读取 records"),
        before,
        "超出总记录上限不得破坏既有 journal"
    );
}

#[tokio::test]
async fn json_depth_limit_is_fail_closed() {
    let test = test_store();
    let session = test.repository.create_with_id("deep-json").await.unwrap();
    let nested = (0..(MAX_JSON_DEPTH + 2)).fold("null".to_owned(), |value, _| format!("[{value}]"));
    let line = format!(
        r#"{{"schema_version":1,"kind":"user","sequence":0,"prompt_id":"p","text":{nested}}}
"#
    );
    std::fs::write(records_path(&test, &session.id), line).expect("写入深层 JSON");

    let error = test.repository.load(&session.id).await.unwrap_err();
    assert_corrupt(error);
}

#[tokio::test]
async fn atomic_append_keeps_previous_content_and_leaves_no_temp_file() {
    let test = test_store();
    let session = test.repository.create_with_id("atomic").await.unwrap();
    let first = user_record("prompt-a", 0);
    let second = assistant_record("prompt-a", 1);
    test.repository
        .append(&session.id, std::slice::from_ref(&first))
        .await
        .unwrap();
    let before = std::fs::read(records_path(&test, &session.id)).expect("读取第一次 journal");

    test.repository
        .append(&session.id, std::slice::from_ref(&second))
        .await
        .unwrap();
    let after = std::fs::read(records_path(&test, &session.id)).expect("读取第二次 journal");
    assert!(after.len() > before.len());
    assert_eq!(
        test.repository.load(&session.id).await.unwrap().records,
        [first, second]
    );
    assert!(
        std::fs::read_dir(session_dir(&test, &session.id))
            .expect("读取 session 目录")
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().contains("tmp")),
        "原子替换失败后不得遗留临时文件"
    );
}

#[tokio::test]
async fn append_inserts_separator_after_a_valid_non_newline_tail() {
    let test = test_store();
    let session = test
        .repository
        .create_with_id("tail-separator")
        .await
        .unwrap();
    let first = user_record("p", 0);
    let second = assistant_record("p", 1);
    test.repository
        .append(&session.id, std::slice::from_ref(&first))
        .await
        .unwrap();

    let mut first_bytes = std::fs::read(records_path(&test, &session.id)).expect("读取 journal");
    assert_eq!(first_bytes.pop(), Some(b'\n'));
    std::fs::write(records_path(&test, &session.id), first_bytes).expect("移除末尾换行");

    test.repository
        .append(&session.id, std::slice::from_ref(&second))
        .await
        .unwrap();
    assert_eq!(
        test.repository.load(&session.id).await.unwrap().records,
        [first, second]
    );
}

#[cfg(unix)]
#[tokio::test]
async fn generated_create_rejects_a_shared_sessions_directory() {
    use std::os::unix::fs::PermissionsExt;

    let test = test_store();
    let sessions_root = test.home.join("efflab-sessions");
    std::fs::create_dir(&sessions_root).expect("创建 sessions 根目录");
    std::fs::set_permissions(&sessions_root, std::fs::Permissions::from_mode(0o755))
        .expect("设置共享 sessions 根目录权限");

    let error = test
        .repository
        .create()
        .await
        .expect_err("共享 sessions 根目录必须拒绝");
    assert_corrupt(error);
    assert!(!sessions_root.join("v1").exists());
}

#[tokio::test]
async fn invalid_sequence_does_not_change_atomic_journal() {
    let test = test_store();
    let session = test.repository.create_with_id("sequence").await.unwrap();
    test.repository
        .append(&session.id, &[user_record("p", 0)])
        .await
        .unwrap();
    let before = std::fs::read(records_path(&test, &session.id)).expect("读取 journal");

    let error = test
        .repository
        .append(&session.id, &[assistant_record("p", 0)])
        .await
        .unwrap_err();
    assert!(matches!(error, SessionError::InvalidRecord));
    assert_eq!(
        std::fs::read(records_path(&test, &session.id)).expect("再次读取 journal"),
        before
    );
}

#[tokio::test]
async fn persisted_record_schema_is_closed_and_contains_no_sensitive_fields() {
    let test = test_store();
    let session = test
        .repository
        .create_with_id("closed-schema")
        .await
        .unwrap();
    let records = [
        user_record("p", 0),
        assistant_record("p", 1),
        SessionRecord::Tool {
            sequence: 2,
            prompt_id: "p".to_owned(),
            round: 0,
            tool_call_id: "call-1".to_owned(),
            name: "safe_tool".to_owned(),
            detail: "safe summary".to_owned(),
            status: "completed".to_owned(),
        },
        SessionRecord::TurnTerminal {
            sequence: 3,
            prompt_id: "p".to_owned(),
            status: "completed".to_owned(),
        },
    ];
    test.repository.append(&session.id, &records).await.unwrap();

    let source = std::fs::read_to_string(records_path(&test, &session.id)).expect("读取持久化记录");
    let forbidden = [
        "token",
        "api_key",
        "authorization",
        "headers",
        "mcp_env",
        "runtime_config",
        "unknown_payload",
    ];
    for line in source.lines() {
        let value: serde_json::Value = serde_json::from_str(line).expect("journal 行必须是 JSON");
        let object = value.as_object().expect("journal 记录必须是 object");
        for key in forbidden {
            assert!(
                !object.contains_key(key),
                "敏感字段 {key} 不得落盘: {object:?}"
            );
        }
    }
}

#[tokio::test]
async fn unknown_record_fields_and_schema_versions_are_rejected() {
    let test = test_store();
    let session = test
        .repository
        .create_with_id("unknown-schema")
        .await
        .unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(manifest_path(&test, &session.id)).unwrap())
            .unwrap();
    manifest["unknown_payload"] = serde_json::json!("must-not-persist");
    std::fs::write(
        manifest_path(&test, &session.id),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert_corrupt(test.repository.load(&session.id).await.unwrap_err());

    let test = test_store();
    let session = test
        .repository
        .create_with_id("unknown-record")
        .await
        .unwrap();
    std::fs::write(
        records_path(&test, &session.id),
        br#"{"schema_version":1,"kind":"user","sequence":0,"prompt_id":"p","text":"x","unknown_payload":"secret"}
"#,
    )
    .unwrap();
    assert_corrupt(test.repository.load(&session.id).await.unwrap_err());

    let test = test_store();
    let session = test
        .repository
        .create_with_id("wrong-version")
        .await
        .unwrap();
    std::fs::write(
        manifest_path(&test, &session.id),
        br#"{"schema_version":2,"session_id":"wrong-version"}
"#,
    )
    .unwrap();
    assert_corrupt(test.repository.load(&session.id).await.unwrap_err());
}

#[tokio::test]
async fn missing_session_is_classified_without_disclosing_path() {
    let test = test_store();
    let error = test.repository.load("missing-session").await.unwrap_err();
    assert!(matches!(error, SessionError::NotFound));
    assert_eq!(error.code(), "session_not_found");
    assert!(!error.to_string().contains(test.home.to_str().unwrap()));
}

#[tokio::test]
async fn legacy_version0_chat_history_is_displayable_and_imported_once() {
    let test = test_store_with_legacy_dir("version0_chat");
    let legacy = legacy_session_dir(&test, "version0_chat");
    let before = legacy_fingerprint(&legacy);

    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert_eq!(
        loaded
            .records
            .iter()
            .filter_map(text_of)
            .collect::<Vec<_>>(),
        ["legacy version zero user", "legacy version zero assistant"]
    );
    assert!(loaded.records.iter().any(has_terminal));
    assert!(loaded.read_only, "chat history has no original prompt ids");
    assert!(manifest_path(&test, "legacy-1").is_file());
    assert_eq!(legacy_fingerprint(&legacy), before);
}

#[tokio::test]
async fn empty_chat_history_is_read_only_without_a_partial_tail() {
    let test = test_store_with_legacy_dir("version0_chat");
    let legacy = legacy_session_dir(&test, "version0_chat");
    std::fs::write(legacy.join("chat_history.jsonl"), []).expect("清空 legacy chat history");

    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.read_only);
    assert!(!loaded.partial_tail);
}

#[tokio::test]
async fn legacy_version1_conversation_items_count_non_transcript_items() {
    let test = test_store_with_legacy_dir("version1_items");
    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::User { text, .. } if text == "legacy version one user")
    }));
    assert!(loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::AssistantSnapshot { text, streaming: false, .. } if text == "legacy version one assistant")
    }));
    assert_eq!(loaded.legacy_unknown_total, 2);
    assert!(loaded.read_only);
}

#[tokio::test]
async fn legacy_missing_prompt_id_is_displayable_but_read_only() {
    let test = test_store_with_legacy_dir("missing_prompt_id");
    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.read_only);
    assert!(loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::AssistantSnapshot { text, .. } if text == "missing prompt id assistant")
    }));
    assert!(
        loaded
            .records
            .iter()
            .filter_map(SessionRecord::prompt_id)
            .any(|prompt_id| prompt_id == "legacy:legacy-1:1")
    );
    let error = ensure_prompt_allowed(&test.repository, "legacy-1")
        .await
        .unwrap_err();
    assert_eq!(error.code(), "legacy_session_read_only");
}

#[tokio::test]
async fn compaction_rewind_and_partial_tail_are_read_only() {
    for fixture in ["compaction", "rewind", "partial_tail"] {
        let test = test_store_with_legacy_dir(fixture);
        let loaded = test.repository.load("legacy-1").await.unwrap();
        assert!(loaded.read_only, "{fixture}");
        assert_eq!(
            ensure_prompt_allowed(&test.repository, "legacy-1")
                .await
                .unwrap_err()
                .code(),
            "legacy_session_read_only",
            "{fixture} prompt must remain read-only"
        );
        if fixture == "compaction" {
            assert_eq!(
                loaded
                    .thinking
                    .iter()
                    .map(|thinking| thinking.text.as_str())
                    .collect::<Vec<_>>(),
                ["compaction thinking"]
            );
            assert!(!loaded.records.iter().any(|record| {
                matches!(
                    record,
                    SessionRecord::AssistantSnapshot { text, .. }
                        if text == "compaction thinking"
                )
            }));
            assert_eq!(loaded.legacy_control_total, 1);
        }
        if fixture == "partial_tail" {
            assert!(loaded.partial_tail);
            assert!(loaded.records.iter().any(|record| {
                matches!(
                    record,
                    SessionRecord::AssistantSnapshot {
                        streaming: false,
                        ..
                    }
                )
            }));
        }
    }
}

#[tokio::test]
async fn summary_conflict_sets_read_only_without_deleting_old_files() {
    let test = test_store_with_legacy_dir("summary_conflict");
    let legacy = legacy_session_dir(&test, "summary_conflict");
    let before = legacy_fingerprint(&legacy);
    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.read_only);
    assert_eq!(legacy_fingerprint(&legacy), before);
    let listed = test.repository.list().await.unwrap();
    assert!(listed[0].read_only, "summary conflict 在 list 中也必须只读");
}

#[tokio::test]
async fn updates_are_authoritative_over_conflicting_chat_history() {
    let test = test_store_with_legacy_dir("summary_conflict");
    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::User { text, .. } if text == "updates source user")
    }));
    assert!(!loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::User { text, .. } if text == "chat fallback user")
    }));
}

#[tokio::test]
async fn legacy_title_metadata_is_exposed_by_list_without_importing_transcript() {
    let test = test_store_with_legacy_dir("version1_items");
    let summaries = test.repository.list().await.unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id, "legacy-1");
    assert_eq!(summaries[0].title.as_deref(), Some("Legacy fixture title"));
}

#[tokio::test]
async fn tool_context_allows_only_expected_ready_qualified_tools() {
    let allowed = test_store_with_legacy_dir("tool_context");
    let expected = BTreeSet::from(["fixture__allowed".to_owned()]);
    let ready = BTreeSet::from(["fixture__allowed".to_owned()]);
    let loaded = allowed
        .repository
        .load_with_tool_policy("legacy-1", &expected, &ready)
        .await
        .unwrap();
    assert!(!loaded.read_only);
    assert!(loaded.records.iter().any(has_tool));

    let disallowed = test_store_with_legacy_dir("tool_context");
    let ready = BTreeSet::from(["fixture__other".to_owned()]);
    let loaded = disallowed
        .repository
        .load_with_tool_policy("legacy-1", &expected, &ready)
        .await
        .unwrap();
    assert!(loaded.read_only);
    assert!(loaded.records.iter().any(has_tool));

    let no_policy = test_store_with_legacy_dir("tool_context");
    let loaded = no_policy.repository.load("legacy-1").await.unwrap();
    assert!(loaded.read_only, "没有当前工具策略时 legacy tool 不得继续");
}

#[tokio::test]
async fn invalid_legacy_qualified_tool_is_preserved_as_read_only_audit_only() {
    let test = test_store_with_legacy_dir("tool_context");
    let legacy = legacy_session_dir(&test, "tool_context");
    let invalid_name = "fixture__bad.name";
    let updates_path = legacy.join("updates.jsonl");
    let updates = std::fs::read_to_string(&updates_path).expect("读取 legacy updates");
    std::fs::write(
        &updates_path,
        updates.replace("fixture__allowed", invalid_name),
    )
    .expect("替换非法 legacy qualified name");

    let expected = BTreeSet::from([invalid_name.to_owned()]);
    let loaded = test
        .repository
        .load_with_tool_policy("legacy-1", &expected, &expected)
        .await
        .expect("非法历史 qualified name 仍应可加载用于审计");
    assert!(
        loaded.read_only,
        "非法历史 qualified name 必须使 session 只读"
    );
    assert!(loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::Tool { name, .. } if name == invalid_name)
    }));
    assert_eq!(
        ensure_prompt_allowed(&test.repository, "legacy-1")
            .await
            .expect_err("只读 legacy session 不得继续 prompt")
            .code(),
        "legacy_session_read_only"
    );
}

#[tokio::test]
async fn orphan_tool_update_is_read_only_without_a_tool_snapshot() {
    let test = test_store_with_legacy_dir("tool_context");
    let legacy = legacy_session_dir(&test, "tool_context");
    let updates =
        std::fs::read_to_string(legacy.join("updates.jsonl")).expect("读取 tool context updates");
    let without_call = updates
        .lines()
        .filter(|line| !line.contains(r#""sessionUpdate":"tool_call""#))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(legacy.join("updates.jsonl"), format!("{without_call}\n"))
        .expect("删除 tool_call 快照");

    let expected = BTreeSet::from(["fixture__allowed".to_owned()]);
    let loaded = test
        .repository
        .load_with_tool_policy("legacy-1", &expected, &expected)
        .await
        .unwrap();
    assert!(loaded.read_only);
}

#[tokio::test]
async fn last_turn_summary_never_enters_transcript_or_partial_tail() {
    let test = test_store_with_legacy_dir("compaction");
    let legacy = legacy_session_dir(&test, "compaction");
    std::fs::write(
        legacy.join("updates.jsonl"),
        br#"{"timestamp":1,"method":"_x.ai/session/update","params":{"sessionId":"legacy-1","_meta":{"promptId":"prompt-summary"},"update":{"sessionUpdate":"last_turn_summary","summary":"not transcript","prompt_id":"prompt-summary"}}}
"#,
    )
    .expect("写入 LastTurnSummary update");

    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.records.is_empty());
    assert!(loaded.thinking.is_empty());
    assert!(loaded.read_only);
    assert!(!loaded.partial_tail);
}

#[tokio::test]
async fn empty_updates_are_read_only_without_a_partial_tail() {
    let test = test_store_with_legacy_dir("compaction");
    let legacy = legacy_session_dir(&test, "compaction");
    std::fs::write(legacy.join("updates.jsonl"), []).expect("清空 legacy updates");

    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.read_only);
    assert!(!loaded.partial_tail);
}

#[tokio::test]
async fn agent_thought_is_displayable_without_entering_continuation_transcript() {
    let test = test_store_with_legacy_dir("tool_context");
    let legacy = legacy_session_dir(&test, "tool_context");
    let updates_path = legacy.join("updates.jsonl");
    let updates = std::fs::read_to_string(&updates_path).expect("读取 legacy updates");
    let thought = r#"{"timestamp":1.5,"method":"session/update","params":{"sessionId":"legacy-1","_meta":{"promptId":"prompt-tool"},"update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"visible thought"}}}}"#;
    let mut lines = updates.lines();
    let first = lines.next().expect("tool fixture 至少有 user update");
    let remainder = lines.collect::<Vec<_>>().join("\n");
    let rewritten = format!("{first}\n{thought}\n{remainder}\n");
    std::fs::write(&updates_path, rewritten).expect("插入 thinking update");

    let expected = BTreeSet::from(["fixture__allowed".to_owned()]);
    let loaded = test
        .repository
        .load_with_tool_policy("legacy-1", &expected, &expected)
        .await
        .unwrap();
    assert!(
        !loaded.read_only,
        "有原始 promptId 的 thinking 不应单独禁用继续"
    );
    assert_eq!(
        loaded
            .thinking
            .iter()
            .map(|thinking| thinking.text.as_str())
            .collect::<Vec<_>>(),
        ["visible thought"]
    );
    assert!(!loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::AssistantSnapshot { text, .. } if text == "visible thought")
    }));
}

#[tokio::test]
async fn thinking_without_a_transcript_boundary_is_read_only() {
    let test = test_store_with_legacy_dir("compaction");
    let legacy = legacy_session_dir(&test, "compaction");
    std::fs::write(
        legacy.join("updates.jsonl"),
        br#"{"timestamp":1,"method":"session/update","params":{"sessionId":"legacy-1","_meta":{"promptId":"prompt-thinking"},"update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"orphan thinking"}}}}
"#,
    )
    .expect("写入孤立 thinking update");

    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.read_only);
    assert_eq!(
        ensure_prompt_allowed(&test.repository, "legacy-1")
            .await
            .unwrap_err()
            .code(),
        "legacy_session_read_only"
    );
}

#[tokio::test]
async fn unknown_legacy_update_tag_fails_closed_without_echoing_data() {
    let test = test_store_with_legacy_dir("compaction");
    let legacy = legacy_session_dir(&test, "compaction");
    let updates_path = legacy.join("updates.jsonl");
    let mut updates = std::fs::read_to_string(&updates_path).expect("读取 legacy updates");
    updates.push_str(
        r#"{"timestamp":99,"method":"session/update","params":{"sessionId":"legacy-1","_meta":{"promptId":"prompt-future"},"update":{"sessionUpdate":"future_unmapped_update","secret":"unknown-legacy-secret"}}}
"#,
    );
    std::fs::write(&updates_path, updates).expect("写入未知 legacy update");

    let error = test.repository.load("legacy-1").await.unwrap_err();
    assert_corrupt(error);
    assert!(!error.to_string().contains("unknown-legacy-secret"));
    assert!(!error.to_string().contains(test.home.to_str().unwrap()));
}

#[tokio::test]
async fn legacy_long_cwd_metadata_file_is_not_treated_as_session() {
    let test = test_store();
    let cwd_component = "legacy-workspace-0123456789abcdef";
    let cwd_path = test.home.join("sessions").join(cwd_component);
    let session_path = cwd_path.join("legacy-1");
    copy_fixture_dir(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy")
            .join("missing_prompt_id"),
        &session_path,
    );
    let summary_path = session_path.join("summary.json");
    let summary = std::fs::read_to_string(&summary_path)
        .expect("读取 long cwd summary")
        .replace("/legacy/workspace", "/legacy/long/workspace");
    std::fs::write(&summary_path, summary).expect("更新 long cwd summary");
    std::fs::write(cwd_path.join(".cwd"), "/legacy/long/workspace").expect("写入 long cwd 元数据");

    let loaded = test.repository.load("legacy-1").await.unwrap();
    assert!(loaded.records.iter().any(|record| {
        matches!(record, SessionRecord::AssistantSnapshot { text, .. } if text == "missing prompt id assistant")
    }));
}

#[tokio::test]
async fn corrupt_legacy_line_inside_a_turn_fails_closed_without_echoing_data() {
    let test = test_store_with_legacy_dir("corrupt_line");
    let error = test.repository.load("legacy-1").await.unwrap_err();
    assert_eq!(error, SessionError::Corrupt);
    assert!(!error.to_string().contains("corrupt-line-secret"));
    assert!(!error.to_string().contains(test.home.to_str().unwrap()));
}

#[tokio::test]
async fn v1_session_wins_over_legacy_after_the_first_import() {
    let test = test_store_with_legacy_dir("version0_chat");
    let first = test.repository.load("legacy-1").await.unwrap();
    let legacy = legacy_session_dir(&test, "version0_chat");
    std::fs::write(
        legacy.join("chat_history.jsonl"),
        br#"{"role":"user","content":"mutated legacy data"}
"#,
    )
    .expect("修改 legacy 测试文件");
    let second = test.repository.load("legacy-1").await.unwrap();
    assert_eq!(second.records, first.records);
}
