//! sidecar v1 本地 session manifest 与原子 records journal。
//!
//! 本模块只保存最小、版本化且已白名单化的 transcript 记录，不读取或持久化 runtime
//! 配置、认证凭据、MCP 环境和未知 ACP payload；同时提供旧 session 的只读懒导入。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, Metadata};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

use efflab_agent_contract::{is_prompt_id, is_qualified_tool_name};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// v1 session 文件允许的最大字节数。
pub const MAX_SESSION_FILE_BYTES: usize = 32 * 1024 * 1024;
/// v1 records.jsonl 单行允许的最大字节数（不含换行符）。
pub const MAX_LINE_BYTES: usize = 256 * 1024;
/// 一个 session 允许的最大记录数。
pub const MAX_RECORDS: usize = 10_000;
/// 单个 JSON 值允许的最大容器嵌套深度。
pub const MAX_JSON_DEPTH: usize = 16;

/// 为后续调用方保留语义更明确的常量别名。
pub const MAX_RECORD_LINE_BYTES: usize = MAX_LINE_BYTES;
/// 为后续调用方保留语义更明确的常量别名。
pub const MAX_SESSION_RECORDS: usize = MAX_RECORDS;
/// 为后续调用方保留语义更明确的常量别名。
pub const SESSION_FILE_LIMIT_BYTES: usize = MAX_SESSION_FILE_BYTES;

const SCHEMA_VERSION: u32 = 1;
const SESSION_ROOT: &str = "efflab-sessions";
const V1_ROOT: &str = "v1";
const LEGACY_SESSIONS_ROOT: &str = "sessions";
const MANIFEST_FILE: &str = "manifest.json";
const RECORDS_FILE: &str = "records.jsonl";
const LEGACY_SUMMARY_FILE: &str = "summary.json";
const LEGACY_UPDATES_FILE: &str = "updates.jsonl";
const LEGACY_CHAT_HISTORY_FILE: &str = "chat_history.jsonl";
const LEGACY_ACP_UPDATE_METHOD: &str = "session/update";
const LEGACY_XAI_UPDATE_METHOD: &str = "_x.ai/session/update";
/// 与 MCP/transcript gate 一致的唯一内置工具例外；不放宽为任意 GrokBuild:*。
const NOOP_TOOL: &str = "GrokBuild:efflab_noop";
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;
const MAX_LEGACY_METADATA_ID_BYTES: usize = 1024;
/// 公开 identifier 的持久化上限，供 MCP catalog 与 transcript 共用同一边界。
pub const MAX_RECORD_ID_BYTES: usize = 1024;
const GENERATED_ID_ATTEMPTS: usize = 128;

static GENERATED_SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);
static LEGACY_IMPORT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 旧 ACP thinking chunk 的展示快照；它不属于可继续的模型 transcript。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThinkingSnapshot {
    /// legacy journal 中的稳定顺序。
    pub sequence: u64,
    /// 原始 prompt id 或按记录位置派生的只读 id。
    pub prompt_id: String,
    /// 可展示的 thinking 文本。
    pub text: String,
}

impl fmt::Debug for ThinkingSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ThinkingSnapshot")
            .field("sequence", &self.sequence)
            .field("prompt_id", &self.prompt_id)
            .field("text_bytes", &self.text.len())
            .finish()
    }
}

/// 传给 legacy importer 的当前工具权限快照。
#[derive(Clone, Default)]
pub struct LegacyToolPolicy {
    expected_tools: BTreeSet<String>,
    ready_tools: BTreeSet<String>,
}

impl LegacyToolPolicy {
    /// 构造 expected 与 ready 的只读交集策略。
    pub fn new(expected_tools: BTreeSet<String>, ready_tools: BTreeSet<String>) -> Self {
        Self {
            expected_tools,
            ready_tools,
        }
    }

    /// 只有安全 qualified 名才能解除 legacy 只读；历史值仍可作为审计记录保全。
    fn allows(&self, name: &str) -> bool {
        (name == NOOP_TOOL || is_qualified_tool_name(name))
            && self.expected_tools.contains(name)
            && self.ready_tools.contains(name)
    }
}

/// v1 或 legacy session 的可加载快照；字段保持公开以供后续 turn loop 复用。
#[derive(Clone, PartialEq, Eq)]
pub struct Session {
    /// session id，同时也是 v1 目录名。
    pub id: String,
    /// 按 journal sequence 顺序排列的已验证记录。
    pub records: Vec<SessionRecord>,
    /// legacy 元数据或不完整边界导致的只读标记。
    pub read_only: bool,
    /// legacy summary/title 解析出的列表标题。
    pub title: Option<String>,
    /// 只展示、不送入模型上下文的 thinking 快照。
    pub thinking: Vec<ThinkingSnapshot>,
    /// 未知 ConversationItem 或无法映射的公开旧记录数量。
    pub legacy_unknown_total: usize,
    /// compaction、rewind、plan、workflow 等控制记录数量。
    pub legacy_control_total: usize,
    /// 可恢复边界外被跳过的损坏行数量。
    pub legacy_corrupt_total: usize,
    /// 最后一个 turn 没有 terminal 的 partial tail。
    pub partial_tail: bool,
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("id", &self.id)
            .field("record_count", &self.records.len())
            .field("read_only", &self.read_only)
            .field("title_present", &self.title.is_some())
            .field("thinking_count", &self.thinking.len())
            .field("legacy_unknown_total", &self.legacy_unknown_total)
            .field("legacy_control_total", &self.legacy_control_total)
            .field("legacy_corrupt_total", &self.legacy_corrupt_total)
            .field("partial_tail", &self.partial_tail)
            .finish()
    }
}

impl Session {
    /// 创建没有 legacy 诊断元数据的 v1 空快照。
    fn empty(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            records: Vec::new(),
            read_only: false,
            title: None,
            thinking: Vec::new(),
            legacy_unknown_total: 0,
            legacy_control_total: 0,
            legacy_corrupt_total: 0,
            partial_tail: false,
        }
    }
}

/// `session/list` 可直接消费的最小摘要。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    /// session id。
    pub id: String,
    /// legacy summary 的展示标题；v1 无标题时为 `None`。
    pub title: Option<String>,
    /// 已导入且不可继续的 legacy session 标记。
    pub read_only: bool,
}

/// 不含原始参数的 assistant 工具调用快照；仅用于安全恢复模型 round。
#[derive(Clone, PartialEq, Eq)]
pub struct ToolCallSnapshot {
    /// 由 sidecar 生成并与对应 tool result 配对的调用 id。
    pub tool_call_id: String,
    /// 已通过工具策略审核的公开工具名。
    pub name: String,
}

/// v1 journal 的白名单记录。
#[derive(Clone, PartialEq, Eq)]
pub enum SessionRecord {
    /// 用户输入；prompt id 必须来自原始 turn。
    User {
        /// journal sequence。
        sequence: u64,
        /// 绑定当前 turn 的 prompt id。
        prompt_id: String,
        /// 已截取的用户文本。
        text: String,
    },
    /// assistant 当前文本块的快照。
    AssistantSnapshot {
        /// journal sequence。
        sequence: u64,
        /// 绑定当前 turn 的 prompt id。
        prompt_id: String,
        /// 文本块 id。
        block_id: String,
        /// 已截取的 assistant 文本。
        text: String,
        /// 是否仍处于流式状态。
        streaming: bool,
    },
    /// assistant 发出的一个工具 round；只保存安全 id/name，不保存模型原始参数。
    AssistantToolCalls {
        /// journal sequence。
        sequence: u64,
        /// 绑定当前 turn 的 prompt id。
        prompt_id: String,
        /// 同一 prompt 内单调递增的工具 round。
        round: u32,
        /// 本 round 的安全工具调用元数据，顺序与 assistant tool_calls 一致。
        tool_calls: Vec<ToolCallSnapshot>,
        /// assistant 在工具调用消息中的可选文本。
        text: String,
    },
    /// 已脱敏的工具调用摘要；不承载原始参数或结果。
    Tool {
        /// journal sequence。
        sequence: u64,
        /// 绑定当前 turn 的 prompt id。
        prompt_id: String,
        /// 同一 prompt 内所属的工具 round。
        #[allow(dead_code)]
        round: u32,
        /// 工具调用 id。
        tool_call_id: String,
        /// 已审核的公开工具名。
        name: String,
        /// 已脱敏的短摘要。
        detail: String,
        /// 工具状态。
        status: String,
    },
    /// 一个 turn 的终态。
    TurnTerminal {
        /// journal sequence。
        sequence: u64,
        /// 绑定当前 turn 的 prompt id。
        prompt_id: String,
        /// `completed` 或 `cancelled` 等公开状态。
        status: String,
    },
    /// 模型上下文压缩摘要；旧记录仍保留给 UI replay，不删除。
    CompactSummary {
        /// journal sequence。
        sequence: u64,
        /// 触发压缩的 prompt id。
        prompt_id: String,
        /// 被摘要前缀的最后一条 journal sequence（含）。
        covered_until_sequence: u64,
        /// 已截取的摘要正文。
        text: String,
    },
}

impl fmt::Debug for SessionRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct(self.kind());
        debug.field("sequence", &self.sequence());
        debug.field("prompt_id", &self.prompt_id());
        match self {
            Self::User { text, .. } => {
                debug.field("text_bytes", &text.len());
            }
            Self::AssistantSnapshot {
                block_id,
                text,
                streaming,
                ..
            } => {
                debug.field("block_id", block_id);
                debug.field("text_bytes", &text.len());
                debug.field("streaming", streaming);
            }
            Self::AssistantToolCalls {
                round,
                tool_calls,
                text,
                ..
            } => {
                debug.field("round", round);
                debug.field("tool_call_count", &tool_calls.len());
                debug.field("text_bytes", &text.len());
            }
            Self::Tool {
                tool_call_id,
                name,
                detail,
                status,
                round,
                ..
            } => {
                debug.field("round", round);
                debug.field("tool_call_id", tool_call_id);
                debug.field("name", name);
                debug.field("detail_bytes", &detail.len());
                debug.field("status", status);
            }
            Self::TurnTerminal { status, .. } => {
                debug.field("status", status);
            }
            Self::CompactSummary {
                covered_until_sequence,
                text,
                ..
            } => {
                debug.field("covered_until_sequence", covered_until_sequence);
                debug.field("text_bytes", &text.len());
            }
        }
        debug.finish()
    }
}

impl SessionRecord {
    /// 构造用户记录，供 turn loop 避免重复拼写字段。
    pub fn user(sequence: u64, prompt_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self::User {
            sequence,
            prompt_id: prompt_id.into(),
            text: text.into(),
        }
    }

    /// 构造 assistant 快照记录。
    pub fn assistant_snapshot(
        sequence: u64,
        prompt_id: impl Into<String>,
        block_id: impl Into<String>,
        text: impl Into<String>,
        streaming: bool,
    ) -> Self {
        Self::AssistantSnapshot {
            sequence,
            prompt_id: prompt_id.into(),
            block_id: block_id.into(),
            text: text.into(),
            streaming,
        }
    }

    /// 构造 assistant 工具 round 快照；调用方只能传入不含参数的 id/name。
    pub fn assistant_tool_calls(
        sequence: u64,
        prompt_id: impl Into<String>,
        round: u32,
        tool_calls: impl IntoIterator<Item = (String, String)>,
        text: impl Into<String>,
    ) -> Self {
        Self::AssistantToolCalls {
            sequence,
            prompt_id: prompt_id.into(),
            round,
            tool_calls: tool_calls
                .into_iter()
                .map(|(tool_call_id, name)| ToolCallSnapshot { tool_call_id, name })
                .collect(),
            text: text.into(),
        }
    }

    /// 构造工具摘要记录，使用默认 round 兼容旧调用方；detail 必须已经脱敏。
    pub fn tool(
        sequence: u64,
        prompt_id: impl Into<String>,
        tool_call_id: impl Into<String>,
        name: impl Into<String>,
        detail: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self::tool_in_round(sequence, prompt_id, 0, tool_call_id, name, detail, status)
    }

    /// 构造带 round 的工具摘要记录；调用方必须先对 detail 脱敏。
    pub fn tool_in_round(
        sequence: u64,
        prompt_id: impl Into<String>,
        round: u32,
        tool_call_id: impl Into<String>,
        name: impl Into<String>,
        detail: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self::Tool {
            sequence,
            prompt_id: prompt_id.into(),
            round,
            tool_call_id: tool_call_id.into(),
            name: name.into(),
            detail: detail.into(),
            status: status.into(),
        }
    }

    /// 构造 turn 终态记录。
    pub fn turn_terminal(
        sequence: u64,
        prompt_id: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self::TurnTerminal {
            sequence,
            prompt_id: prompt_id.into(),
            status: status.into(),
        }
    }

    /// 构造模型上下文压缩摘要；调用方必须先截断正文。
    pub fn compact_summary(
        sequence: u64,
        prompt_id: impl Into<String>,
        covered_until_sequence: u64,
        text: impl Into<String>,
    ) -> Self {
        Self::CompactSummary {
            sequence,
            prompt_id: prompt_id.into(),
            covered_until_sequence,
            text: text.into(),
        }
    }

    /// 覆盖 journal sequence；只允许 store 在 append 时调用。
    fn with_sequence(mut self, sequence: u64) -> Self {
        match &mut self {
            Self::User { sequence: slot, .. }
            | Self::AssistantSnapshot { sequence: slot, .. }
            | Self::AssistantToolCalls { sequence: slot, .. }
            | Self::Tool { sequence: slot, .. }
            | Self::TurnTerminal { sequence: slot, .. }
            | Self::CompactSummary { sequence: slot, .. } => *slot = sequence,
        }
        self
    }

    /// 返回与当前记录绑定的 prompt id；所有 v1 记录都必须有值。
    pub fn prompt_id(&self) -> Option<&str> {
        match self {
            Self::User { prompt_id, .. }
            | Self::AssistantSnapshot { prompt_id, .. }
            | Self::AssistantToolCalls { prompt_id, .. }
            | Self::Tool { prompt_id, .. }
            | Self::TurnTerminal { prompt_id, .. }
            | Self::CompactSummary { prompt_id, .. } => Some(prompt_id),
        }
    }

    /// 返回 journal sequence。
    pub fn sequence(&self) -> u64 {
        match self {
            Self::User { sequence, .. }
            | Self::AssistantSnapshot { sequence, .. }
            | Self::AssistantToolCalls { sequence, .. }
            | Self::Tool { sequence, .. }
            | Self::TurnTerminal { sequence, .. }
            | Self::CompactSummary { sequence, .. } => *sequence,
        }
    }

    /// 返回稳定的记录类型名称。
    pub fn kind(&self) -> &'static str {
        match self {
            Self::User { .. } => "User",
            Self::AssistantSnapshot { .. } => "AssistantSnapshot",
            Self::AssistantToolCalls { .. } => "AssistantToolCalls",
            Self::Tool { .. } => "Tool",
            Self::TurnTerminal { .. } => "TurnTerminal",
            Self::CompactSummary { .. } => "CompactSummary",
        }
    }
}

/// session store 的稳定错误分类；不携带路径、秘密或记录正文。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    /// 调用方记录、session id 或写入批次不符合合同。
    InvalidRecord,
    /// session 或其 v1 根目录不存在。
    NotFound,
    /// 未来 legacy 适配器发现只读 session 时使用。
    LegacyReadOnly,
    /// manifest、journal、路径或 schema 损坏。
    Corrupt,
    /// 文件系统或平台能力失败。
    Io,
}

impl SessionError {
    /// 返回可供 Host/turn loop 稳定匹配的错误码。
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRecord => "invalid_record",
            Self::NotFound => "session_not_found",
            Self::LegacyReadOnly => "legacy_session_read_only",
            Self::Corrupt => "session_corrupt",
            Self::Io => "session_io",
        }
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for SessionError {}

/// 持有 sidecar home 的 v1 session repository；同一实例内串行化写入。
#[derive(Clone)]
pub struct SessionRepository {
    home: PathBuf,
    operation_lock: Arc<Mutex<()>>,
}

impl SessionRepository {
    /// 创建 repository；实际路径与权限校验延迟到异步操作，以保持构造函数无错误分支。
    pub fn new(home: impl Into<PathBuf>) -> Self {
        Self {
            home: home.into(),
            operation_lock: Arc::new(Mutex::new(())),
        }
    }

    /// 返回 repository home 的只读引用，供 runtime 绑定路径时使用。
    pub fn home(&self) -> &Path {
        &self.home
    }

    /// 创建一个新的 v1 session，并返回空快照。
    pub async fn create(&self) -> Result<Session, SessionError> {
        let repository = self.clone();
        run_blocking(move || repository.create_generated_sync()).await
    }

    /// 使用调用方提供的合法 id 创建 session，供 ACP session/new 保持 id 映射。
    pub async fn create_with_id(&self, session_id: &str) -> Result<Session, SessionError> {
        let repository = self.clone();
        let session_id = session_id.to_owned();
        run_blocking(move || repository.create_with_id_sync(&session_id)).await
    }

    /// 列出并完整校验 v1 session；结果按 id 字典序返回。
    pub async fn list(&self) -> Result<Vec<SessionSummary>, SessionError> {
        let repository = self.clone();
        run_blocking(move || repository.list_sync()).await
    }

    /// 加载 v1 或按需导入 legacy session；任何 torn/未知/超限状态都 fail-closed。
    pub async fn load(&self, session_id: &str) -> Result<Session, SessionError> {
        self.load_with_tool_policy(session_id, &BTreeSet::new(), &BTreeSet::new())
            .await
    }

    /// 使用当前 expected 与 ready 工具集合加载 session。
    pub async fn load_with_tool_policy(
        &self,
        session_id: &str,
        expected_tools: &BTreeSet<String>,
        ready_tools: &BTreeSet<String>,
    ) -> Result<Session, SessionError> {
        let repository = self.clone();
        let session_id = session_id.to_owned();
        let policy = LegacyToolPolicy::new(expected_tools.clone(), ready_tools.clone());
        run_blocking(move || repository.load_sync(&session_id, &policy)).await
    }

    /// 在真正发送新 prompt 前检查 session 是否仍可继续。
    pub async fn ensure_prompt_allowed(&self, session_id: &str) -> Result<(), SessionError> {
        let session = self.load(session_id).await?;
        if session.read_only {
            tracing::debug!(
                event = "legacy_prompt_rejected",
                "只读 legacy session 拒绝新 prompt"
            );
            return Err(SessionError::LegacyReadOnly);
        }
        Ok(())
    }

    /// 兼容调用方对 prompt 门禁的简短命名。
    pub async fn prompt_read_only(&self, session_id: &str) -> Result<(), SessionError> {
        self.ensure_prompt_allowed(session_id).await
    }

    /// 把一批已白名单化的记录以原子 journal 替换方式追加。
    pub async fn append(
        &self,
        session_id: &str,
        records: &[SessionRecord],
    ) -> Result<(), SessionError> {
        let repository = self.clone();
        let session_id = session_id.to_owned();
        let records = records.to_vec();
        run_blocking(move || repository.append_sync(&session_id, &records)).await
    }

    /// 删除 v1 或可列出的 legacy session 目录；未知 id 返回 NotFound。
    pub async fn delete(&self, session_id: &str) -> Result<(), SessionError> {
        let repository = self.clone();
        let session_id = session_id.to_owned();
        run_blocking(move || repository.delete_sync(&session_id)).await
    }

    /// 生成不含路径和秘密的 session id，并在冲突时重试有限次数。
    fn create_generated_sync(&self) -> Result<Session, SessionError> {
        let _guard = self.lock()?;
        ensure_storage_platform()?;
        validate_home_path(&self.home)?;
        ensure_private_directory(&self.home)?;
        ensure_private_directory(&self.sessions_root())?;
        let v1_root = self.v1_root();
        ensure_private_directory(&v1_root)?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|_| {
                tracing::debug!(
                    event = "session_clock_invalid",
                    "生成 session id 的时钟不可用"
                );
                SessionError::Io
            })?;
        for _ in 0..GENERATED_ID_ATTEMPTS {
            let counter = GENERATED_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
            let session_id = format!("session-{timestamp:x}-{counter:x}");
            if !entry_exists(&self.session_dir(&session_id))? {
                return self.create_session_locked(&session_id);
            }
        }
        tracing::debug!(
            event = "session_create_id_exhausted",
            "生成 session id 失败"
        );
        Err(SessionError::Io)
    }

    /// 创建指定 id 的 session；已存在目录一律不覆盖。
    fn create_with_id_sync(&self, session_id: &str) -> Result<Session, SessionError> {
        let _guard = self.lock()?;
        ensure_storage_platform()?;
        validate_home_path(&self.home)?;
        validate_session_id(session_id)?;
        ensure_private_directory(&self.home)?;
        ensure_private_directory(&self.sessions_root())?;
        ensure_private_directory(&self.v1_root())?;
        self.create_session_locked(session_id)
    }

    /// 在调用方已持有 repository 锁时创建目录、manifest 和空 journal。
    fn create_session_locked(&self, session_id: &str) -> Result<Session, SessionError> {
        let directory = self.session_dir(session_id);
        if entry_exists(&directory)? {
            tracing::debug!(event = "session_create_existing", "拒绝覆盖既有 session");
            return Err(SessionError::Corrupt);
        }
        create_private_directory(&directory)?;
        let manifest = Manifest {
            schema_version: SCHEMA_VERSION,
            session_id: session_id.to_owned(),
            title: None,
            read_only: false,
            thinking: Vec::new(),
            legacy_unknown_total: 0,
            legacy_control_total: 0,
            legacy_corrupt_total: 0,
            partial_tail: false,
        };
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| SessionError::Io)?;
        atomic_write(&directory.join(MANIFEST_FILE), &manifest_bytes)?;
        atomic_write(&directory.join(RECORDS_FILE), &[])?;
        tracing::debug!(event = "session_created", "v1 session 已创建");
        Ok(Session::empty(session_id))
    }

    /// 列出并完整校验 v1 session，同时发现可列出的 legacy summary 目录。
    fn list_sync(&self) -> Result<Vec<SessionSummary>, SessionError> {
        let _guard = self.lock()?;
        ensure_storage_platform()?;
        validate_home_path(&self.home)?;
        let mut summaries = BTreeMap::new();

        if let Some(v1_root) = self.existing_v1_root()? {
            let entries =
                fs::read_dir(v1_root).map_err(|error| map_directory_error(error, false))?;
            for entry in entries {
                let entry = entry.map_err(|_| SessionError::Io)?;
                let path = entry.path();
                let name = entry.file_name();
                let Some(session_id) = name.to_str() else {
                    tracing::debug!(event = "session_list_invalid_id", "session id 不是 UTF-8");
                    return Err(SessionError::Corrupt);
                };
                validate_session_id(session_id).map_err(|_| SessionError::Corrupt)?;
                verify_directory(&path)?;
                let loaded = self.load_v1_locked(session_id)?;
                summaries.insert(
                    session_id.to_owned(),
                    SessionSummary {
                        id: session_id.to_owned(),
                        title: loaded.title,
                        read_only: loaded.read_only,
                    },
                );
            }
        }

        let legacy_candidates = self.list_legacy_candidates()?;
        let mut legacy_counts = BTreeMap::new();
        for candidate in &legacy_candidates {
            *legacy_counts
                .entry(candidate.session_id.clone())
                .or_insert(0usize) += 1;
        }
        for candidate in legacy_candidates {
            if summaries.contains_key(&candidate.session_id) {
                continue;
            }
            let metadata = read_legacy_summary(&candidate.path, &candidate.session_id)?;
            let summary_conflict = !legacy_summary_matches_candidate(&candidate, &metadata)?;
            let duplicate = legacy_counts
                .get(&candidate.session_id)
                .is_some_and(|count| *count > 1);
            summaries.insert(
                candidate.session_id.clone(),
                SessionSummary {
                    id: candidate.session_id,
                    title: metadata.title,
                    read_only: metadata.read_only || summary_conflict || duplicate,
                },
            );
        }

        let result = summaries.into_values().collect::<Vec<_>>();
        tracing::debug!(
            event = "sessions_listed",
            count = result.len(),
            "v1/legacy sessions 已列出"
        );
        Ok(result)
    }

    /// 加载并校验 v1 或按需导入 legacy session；公开 load 已经持有 repository 锁。
    fn load_sync(
        &self,
        session_id: &str,
        policy: &LegacyToolPolicy,
    ) -> Result<Session, SessionError> {
        let _guard = self.lock()?;
        ensure_storage_platform()?;
        validate_home_path(&self.home)?;
        validate_session_id(session_id)?;
        self.load_locked(session_id, policy)
    }

    /// 在锁内优先读取 v1；缺少对应 v1 目录时才扫描并导入 legacy。
    fn load_locked(
        &self,
        session_id: &str,
        policy: &LegacyToolPolicy,
    ) -> Result<Session, SessionError> {
        if self.v1_session_exists(session_id)? {
            return self.load_v1_locked(session_id);
        }
        let Some(candidate) = self.find_legacy_candidate(session_id)? else {
            return Err(SessionError::NotFound);
        };
        self.import_legacy_locked(&candidate, policy)
    }

    /// 读取 v1 manifest 与 records，避免 append 期间读到半套状态。
    fn load_v1_locked(&self, session_id: &str) -> Result<Session, SessionError> {
        let Some(_) = self.existing_v1_root()? else {
            return Err(SessionError::NotFound);
        };
        let directory = self.session_dir(session_id);
        match verify_directory(&directory) {
            Ok(()) => {}
            Err(SessionError::NotFound) => return Err(SessionError::NotFound),
            Err(error) => return Err(error),
        }
        let manifest_path = directory.join(MANIFEST_FILE);
        let manifest_bytes = read_bounded_file(&manifest_path)?;
        validate_json_depth(&manifest_bytes)?;
        let manifest: Manifest = match serde_json::from_slice(&manifest_bytes) {
            Ok(manifest) => manifest,
            Err(_) => {
                tracing::debug!(
                    event = "session_manifest_parse_failed",
                    "session manifest JSON 解析失败"
                );
                return Err(SessionError::Corrupt);
            }
        };
        if manifest.schema_version != SCHEMA_VERSION || manifest.session_id != session_id {
            tracing::debug!(event = "session_manifest_invalid", "拒绝非法 v1 manifest");
            return Err(SessionError::Corrupt);
        }
        validate_session_metadata(&manifest)?;

        let records_path = directory.join(RECORDS_FILE);
        let records_bytes = read_bounded_file(&records_path)?;
        let records = parse_records(&records_bytes)?;
        tracing::debug!(
            event = "session_loaded",
            record_count = records.len(),
            "v1 session 已加载"
        );
        Ok(Session {
            id: session_id.to_owned(),
            records,
            read_only: manifest.read_only,
            title: manifest.title,
            thinking: manifest.thinking,
            legacy_unknown_total: manifest.legacy_unknown_total,
            legacy_control_total: manifest.legacy_control_total,
            legacy_corrupt_total: manifest.legacy_corrupt_total,
            partial_tail: manifest.partial_tail,
        })
    }

    /// 删除已校验 id 的 v1 目录，或对应的可列出 legacy 目录。
    fn delete_sync(&self, session_id: &str) -> Result<(), SessionError> {
        let _guard = self.lock()?;
        ensure_storage_platform()?;
        validate_home_path(&self.home)?;
        validate_session_id(session_id)?;
        if self.v1_session_exists(session_id)? {
            let directory = self.session_dir(session_id);
            fs::remove_dir_all(&directory).map_err(|_| {
                tracing::debug!(event = "session_delete_failed", "删除 v1 session 目录失败");
                SessionError::Io
            })?;
            tracing::debug!(event = "session_deleted", "v1 session 已删除");
            return Ok(());
        }
        if let Some(candidate) = self.find_legacy_candidate(session_id)? {
            fs::remove_dir_all(&candidate.path).map_err(|_| {
                tracing::debug!(
                    event = "legacy_session_delete_failed",
                    "删除 legacy session 目录失败"
                );
                SessionError::Io
            })?;
            tracing::debug!(event = "legacy_session_deleted", "legacy session 已删除");
            return Ok(());
        }
        Err(SessionError::NotFound)
    }

    /// 校验旧 journal 后合并新记录，再执行同目录原子替换。
    fn append_sync(&self, session_id: &str, records: &[SessionRecord]) -> Result<(), SessionError> {
        let _guard = self.lock()?;
        ensure_storage_platform()?;
        validate_home_path(&self.home)?;
        validate_session_id(session_id)?;
        let session = self.load_locked(session_id, &LegacyToolPolicy::default())?;
        if session.read_only {
            tracing::debug!(
                event = "session_append_read_only",
                "只读 session 拒绝追加记录"
            );
            return Err(SessionError::LegacyReadOnly);
        }
        if session.records.len().saturating_add(records.len()) > MAX_RECORDS {
            tracing::debug!(
                event = "session_append_record_limit",
                "拒绝超出 session 记录上限"
            );
            return Err(SessionError::InvalidRecord);
        }

        // sequence 由 store 盖章；调用方传入的值只作占位，避免 turn loop 预分配撞号。
        let mut next_sequence = match session.records.last().map(SessionRecord::sequence) {
            Some(previous) => previous
                .checked_add(1)
                .ok_or(SessionError::InvalidRecord)?,
            None => 0,
        };
        let mut encoded = Vec::with_capacity(records.len());
        for record in records {
            let stamped = record.clone().with_sequence(next_sequence);
            validate_record(&stamped)?;
            let wire =
                PersistedRecord::try_from(&stamped).map_err(|_| SessionError::InvalidRecord)?;
            let line = serde_json::to_vec(&wire).map_err(|_| SessionError::InvalidRecord)?;
            if line.len() > MAX_LINE_BYTES {
                tracing::debug!(
                    event = "session_append_line_limit",
                    "拒绝超出单行上限的记录"
                );
                return Err(SessionError::InvalidRecord);
            }
            encoded.extend_from_slice(&line);
            encoded.push(b'\n');
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or(SessionError::InvalidRecord)?;
        }
        if records.is_empty() {
            return Ok(());
        }

        let current = read_bounded_file(&self.records_path(session_id))?;
        let separator_bytes = usize::from(!current.is_empty() && !current.ends_with(b"\n"));
        let replacement_len = current
            .len()
            .saturating_add(separator_bytes)
            .saturating_add(encoded.len());
        if replacement_len > MAX_SESSION_FILE_BYTES {
            tracing::debug!(
                event = "session_append_file_limit",
                "拒绝超出 session 文件上限"
            );
            return Err(SessionError::InvalidRecord);
        }
        let mut replacement = Vec::with_capacity(replacement_len);
        replacement.extend_from_slice(&current);
        if separator_bytes != 0 {
            replacement.push(b'\n');
        }
        replacement.extend_from_slice(&encoded);
        atomic_write(&self.records_path(session_id), &replacement)?;
        tracing::debug!(
            event = "session_appended",
            record_count = records.len(),
            "v1 records journal 已原子追加"
        );
        Ok(())
    }

    /// 判断目标 v1 session 是否存在；目标存在但损坏时不回退到 legacy。
    fn v1_session_exists(&self, session_id: &str) -> Result<bool, SessionError> {
        let Some(v1_root) = self.existing_v1_root()? else {
            return Ok(false);
        };
        let directory = v1_root.join(session_id);
        match fs::symlink_metadata(&directory) {
            Ok(_) => {
                verify_directory(&directory)?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(_) => Err(SessionError::Io),
        }
    }

    /// 只扫描存在 summary.json 的旧目录，沿用旧 shell 的可列出判定。
    fn list_legacy_candidates(&self) -> Result<Vec<LegacyCandidate>, SessionError> {
        let Some(sessions_root) = existing_legacy_sessions_root(&self.home)? else {
            return Ok(Vec::new());
        };
        let mut candidates = Vec::new();
        let mut cwd_entries = fs::read_dir(sessions_root)
            .map_err(|error| map_directory_error(error, false))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| SessionError::Io)?;
        cwd_entries.sort_by_key(|entry| entry.file_name());
        for cwd_entry in cwd_entries {
            let cwd_path = cwd_entry.path();
            verify_legacy_directory(&cwd_path)?;
            let cwd_component = cwd_entry
                .file_name()
                .to_str()
                .ok_or(SessionError::Corrupt)?
                .to_owned();
            let mut session_entries = fs::read_dir(&cwd_path)
                .map_err(|_| SessionError::Io)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| SessionError::Io)?;
            session_entries.sort_by_key(|entry| entry.file_name());
            for session_entry in session_entries {
                if session_entry.file_name() == std::ffi::OsStr::new(".cwd") {
                    // 长 cwd 的真实旧布局在 cwd 目录旁保存一个可读 `.cwd` 元数据文件。
                    let metadata = fs::symlink_metadata(session_entry.path()).map_err(|error| {
                        if error.kind() == io::ErrorKind::NotFound {
                            SessionError::Corrupt
                        } else {
                            SessionError::Io
                        }
                    })?;
                    verify_legacy_file_metadata(&metadata)?;
                    continue;
                }
                let session_id = session_entry
                    .file_name()
                    .to_str()
                    .ok_or(SessionError::Corrupt)?
                    .to_owned();
                validate_session_id(&session_id).map_err(|_| SessionError::Corrupt)?;
                let session_path = session_entry.path();
                verify_legacy_directory(&session_path)?;
                if legacy_summary_file_exists(&session_path)? {
                    candidates.push(LegacyCandidate {
                        path: session_path,
                        session_id,
                        cwd_component: cwd_component.clone(),
                        duplicate: false,
                    });
                }
            }
        }
        Ok(candidates)
    }

    /// 按稳定目录顺序定位 legacy session；重复 cwd 会让结果进入只读模式。
    fn find_legacy_candidate(
        &self,
        session_id: &str,
    ) -> Result<Option<LegacyCandidate>, SessionError> {
        let mut candidates = self
            .list_legacy_candidates()?
            .into_iter()
            .filter(|candidate| candidate.session_id == session_id)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(None);
        }
        let duplicate = candidates.len() > 1;
        let mut candidate = candidates.remove(0);
        candidate.duplicate = duplicate;
        Ok(Some(candidate))
    }

    /// 首次 legacy load 创建一个完整的 v1 目录；旧目录始终只读保留。
    fn import_legacy_locked(
        &self,
        candidate: &LegacyCandidate,
        policy: &LegacyToolPolicy,
    ) -> Result<Session, SessionError> {
        let summary = read_legacy_summary(&candidate.path, &candidate.session_id)?;
        let summary_conflict = !legacy_summary_matches_candidate(candidate, &summary)?;
        let mut imported = if legacy_file_exists(&candidate.path, LEGACY_UPDATES_FILE)? {
            parse_legacy_updates(&candidate.path, &candidate.session_id, &summary, policy)?
        } else if legacy_file_exists(&candidate.path, LEGACY_CHAT_HISTORY_FILE)? {
            parse_legacy_chat_history(&candidate.path, &candidate.session_id, &summary)?
        } else {
            LegacyParseResult {
                records: Vec::new(),
                thinking: Vec::new(),
                read_only: true,
                legacy_unknown_total: 0,
                legacy_control_total: 0,
                legacy_corrupt_total: 0,
                partial_tail: false,
            }
        };
        imported.read_only |= summary.read_only || summary_conflict || candidate.duplicate;
        let session = Session {
            id: candidate.session_id.clone(),
            records: imported.records,
            read_only: imported.read_only,
            title: summary.title,
            thinking: imported.thinking,
            legacy_unknown_total: imported.legacy_unknown_total,
            legacy_control_total: imported.legacy_control_total,
            legacy_corrupt_total: imported.legacy_corrupt_total,
            partial_tail: imported.partial_tail,
        };
        self.write_imported_v1_locked(&session)?;
        tracing::debug!(
            event = "legacy_session_imported",
            record_count = session.records.len(),
            read_only = session.read_only,
            "legacy session 已导入 v1"
        );
        Ok(session)
    }

    /// 以临时 v1 目录加目录 rename 的方式原子发布 legacy 导入结果。
    fn write_imported_v1_locked(&self, session: &Session) -> Result<(), SessionError> {
        ensure_private_directory(&self.home)?;
        // 旧 sessions 根目录可能沿用旧 shell 的权限；只要求它是真目录，
        // 新建的 v1 根目录仍然使用 owner-only 权限。
        verify_legacy_directory(&self.home.join(LEGACY_SESSIONS_ROOT))?;
        ensure_private_directory(&self.v1_root())?;
        let final_dir = self.session_dir(&session.id);
        if entry_exists(&final_dir)? {
            return Ok(());
        }
        let counter = LEGACY_IMPORT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temporary_dir = self
            .v1_root()
            .join(format!(".{}-import-{counter}", session.id));
        if entry_exists(&temporary_dir)? {
            return Err(SessionError::Io);
        }
        create_private_directory(&temporary_dir)?;
        let result = (|| {
            let manifest = Manifest::from_session(session);
            let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| SessionError::Io)?;
            let records_bytes = encode_records(&session.records)?;
            if records_bytes.len() > MAX_SESSION_FILE_BYTES {
                return Err(SessionError::Corrupt);
            }
            atomic_write(&temporary_dir.join(MANIFEST_FILE), &manifest_bytes)?;
            atomic_write(&temporary_dir.join(RECORDS_FILE), &records_bytes)?;
            fs::rename(&temporary_dir, &final_dir).map_err(|_| SessionError::Io)?;
            sync_directory(&self.v1_root())?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary_dir);
        }
        result
    }

    /// 返回 sessions 根目录。
    fn sessions_root(&self) -> PathBuf {
        self.home.join(SESSION_ROOT)
    }

    /// 返回 v1 根目录。
    fn v1_root(&self) -> PathBuf {
        self.sessions_root().join(V1_ROOT)
    }

    /// 返回受 id 校验保护的 session 目录。
    fn session_dir(&self, session_id: &str) -> PathBuf {
        self.v1_root().join(session_id)
    }

    /// 返回受 id 校验保护的 records 路径。
    fn records_path(&self, session_id: &str) -> PathBuf {
        self.session_dir(session_id).join(RECORDS_FILE)
    }

    /// 只打开现有 v1 根，缺失时由 list/load 分别解释为 empty/not-found。
    fn existing_v1_root(&self) -> Result<Option<PathBuf>, SessionError> {
        match verify_directory(&self.home) {
            Ok(()) => {}
            Err(SessionError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        }
        // 读取 legacy 时不能要求旧 sessions 根已经满足 v1 的 0700；
        // v1 自己的根目录仍在下方单独执行严格权限校验。
        match verify_legacy_directory(&self.sessions_root()) {
            Ok(()) => {}
            Err(SessionError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        }
        match verify_directory(&self.v1_root()) {
            Ok(()) => Ok(Some(self.v1_root())),
            Err(SessionError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// 获取 repository 级锁；锁 poisoning 时保持 fail-closed。
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ()>, SessionError> {
        self.operation_lock.lock().map_err(|_| SessionError::Io)
    }
}

/// 在 Tokio blocking pool 执行受限的文件系统操作，避免阻塞 sidecar actor。
async fn run_blocking<T, F>(operation: F) -> Result<T, SessionError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SessionError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation).await.map_err(|_| {
        tracing::debug!(
            event = "session_blocking_task_failed",
            "session 文件任务异常退出"
        );
        SessionError::Io
    })?
}

/// 校验固定 session id，杜绝路径分隔符、dot segment 和 Unicode 绕过。
fn validate_session_id(session_id: &str) -> Result<(), SessionError> {
    let length = session_id.len();
    if !(1..=128).contains(&length)
        || !session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        tracing::debug!(event = "session_id_invalid", "拒绝非法 session id");
        return Err(SessionError::InvalidRecord);
    }
    Ok(())
}

/// 校验 repository home 是绝对、无 dot segment 且可安全逐级检查的路径。
fn validate_home_path(path: &Path) -> Result<(), SessionError> {
    if !path.is_absolute()
        || path.to_str().is_none()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        tracing::debug!(
            event = "session_home_path_invalid",
            "拒绝非法 session home 路径"
        );
        return Err(SessionError::InvalidRecord);
    }
    let component_count = path
        .components()
        .filter(|component| !matches!(component, Component::RootDir | Component::Prefix(_)))
        .count();
    if component_count == 0 {
        tracing::debug!(
            event = "session_home_root_rejected",
            "拒绝把文件系统根作为 session home"
        );
        return Err(SessionError::InvalidRecord);
    }
    Ok(())
}

/// 当前 sidecar 的非 Unix hardening 尚未 proven，因此存储也保持 fail-closed。
fn ensure_storage_platform() -> Result<(), SessionError> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tracing::debug!(
            event = "session_store_platform_unavailable",
            "当前平台没有已验证的 session 存储安全能力"
        );
        Err(SessionError::Io)
    }
}

/// 逐级创建私有目录，并拒绝现有 symlink 或共享权限目录。
fn ensure_private_directory(path: &Path) -> Result<(), SessionError> {
    validate_home_path(path)?;
    let components = path.components().collect::<Vec<_>>();
    let mut current = PathBuf::new();
    for component in components {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                verify_directory_metadata(&metadata)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&current).map_err(|create_error| {
                    if create_error.kind() == io::ErrorKind::AlreadyExists {
                        SessionError::Corrupt
                    } else {
                        SessionError::Io
                    }
                })?;
                set_private_directory_mode(&current)?;
                let metadata = fs::symlink_metadata(&current).map_err(|_| SessionError::Io)?;
                verify_directory_metadata(&metadata)?;
            }
            Err(_) => return Err(SessionError::Io),
        }
    }
    verify_private_directory(&current)
}

/// 创建 session 最终目录，不允许覆盖任何既有目录项。
fn create_private_directory(path: &Path) -> Result<(), SessionError> {
    fs::create_dir(path).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            SessionError::Corrupt
        } else {
            SessionError::Io
        }
    })?;
    set_private_directory_mode(path)?;
    verify_private_directory(path)
}

/// 校验目录路径上的每一级目录项都不是 symlink。
fn verify_directory_components(path: &Path) -> Result<(), SessionError> {
    validate_home_path(path)?;
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                SessionError::NotFound
            } else {
                SessionError::Io
            }
        })?;
        verify_directory_metadata(&metadata)?;
    }
    Ok(())
}

/// 校验现有目录不是 symlink 且使用 owner-only 权限。
fn verify_directory(path: &Path) -> Result<(), SessionError> {
    verify_directory_components(path)?;
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            SessionError::NotFound
        } else {
            SessionError::Io
        }
    })?;
    verify_directory_metadata(&metadata)?;
    verify_private_directory(path)
}

/// 校验目录元数据；先检查 symlink 再检查常规目录类型。
fn verify_directory_metadata(metadata: &Metadata) -> Result<(), SessionError> {
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        tracing::debug!(
            event = "session_directory_invalid",
            "session 路径组件不是安全目录"
        );
        return Err(SessionError::Corrupt);
    }
    Ok(())
}

/// 校验最终私有目录权限；Windows 由上层 capability gate fail-closed。
fn verify_private_directory(path: &Path) -> Result<(), SessionError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| SessionError::Io)?;
    verify_directory_metadata(&metadata)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o7777 != PRIVATE_DIRECTORY_MODE {
            tracing::debug!(
                event = "session_directory_mode_invalid",
                "session 目录权限不是 0700"
            );
            return Err(SessionError::Corrupt);
        }
    }
    Ok(())
}

/// 新建目录后立即收紧权限；不修复预先存在的共享目录。
fn set_private_directory_mode(path: &Path) -> Result<(), SessionError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE))
            .map_err(|_| SessionError::Io)?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// 判断目录项是否存在，同时把 symlink 留给后续安全校验处理。
fn entry_exists(path: &Path) -> Result<bool, SessionError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(SessionError::Io),
    }
}

/// 读取固定上限的私有常规文件，拒绝 symlink、硬链接和不安全权限。
fn read_bounded_file(path: &Path) -> Result<Vec<u8>, SessionError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            tracing::debug!(event = "session_file_missing", "session 文件缺失");
            SessionError::Corrupt
        } else {
            tracing::debug!(
                event = "session_file_metadata_failed",
                "读取 session 文件元数据失败"
            );
            SessionError::Io
        }
    })?;
    verify_regular_file_metadata(&metadata)?;
    if metadata.len() > MAX_SESSION_FILE_BYTES as u64 {
        tracing::debug!(event = "session_file_limit", "session 文件超过 32 MiB 上限");
        return Err(SessionError::Corrupt);
    }
    #[cfg(unix)]
    {
        use std::io::Read as _;
        use std::os::unix::fs::OpenOptionsExt;

        // O_NOFOLLOW 让最终文件项在检查与读取之间被替换为 symlink 时也拒绝打开。
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| {
                tracing::debug!(event = "session_file_open_failed", "打开 session 文件失败");
                SessionError::Io
            })?;
        let opened_metadata = file.metadata().map_err(|_| {
            tracing::debug!(
                event = "session_file_metadata_failed",
                "读取 session 文件元数据失败"
            );
            SessionError::Io
        })?;
        verify_regular_file_metadata(&opened_metadata)?;
        if opened_metadata.len() > MAX_SESSION_FILE_BYTES as u64 {
            tracing::debug!(
                event = "session_file_raced_limit",
                "打开后发现 session 文件超限"
            );
            return Err(SessionError::Corrupt);
        }
        let mut bytes = Vec::new();
        file.take(MAX_SESSION_FILE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| {
                tracing::debug!(event = "session_file_read_failed", "读取 session 文件失败");
                SessionError::Io
            })?;
        if bytes.len() > MAX_SESSION_FILE_BYTES {
            tracing::debug!(
                event = "session_file_raced_limit",
                "读取后发现 session 文件超限"
            );
            return Err(SessionError::Corrupt);
        }
        return Ok(bytes);
    }

    #[cfg(not(unix))]
    {
        ensure_storage_platform()?;
        Err(SessionError::Io)
    }
}

/// 校验文件类型、Unix owner-only mode 和单 inode 链接数。
fn verify_regular_file_metadata(metadata: &Metadata) -> Result<(), SessionError> {
    let file_type = metadata.file_type();
    if file_type.is_symlink() || !file_type.is_file() {
        tracing::debug!(
            event = "session_file_type_invalid",
            "session 文件不是常规文件"
        );
        return Err(SessionError::Corrupt);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.nlink() != 1 || metadata.permissions().mode() & 0o7777 != PRIVATE_FILE_MODE {
            tracing::debug!(
                event = "session_file_security_invalid",
                "session 文件权限或链接数不安全"
            );
            return Err(SessionError::Corrupt);
        }
    }
    Ok(())
}

/// 通过已有 hardening helper 完成同目录临时文件、sync、原子替换和父目录 sync。
fn atomic_write(path: &Path, content: &[u8]) -> Result<(), SessionError> {
    tracing::debug!(
        event = "session_atomic_write_started",
        bytes = content.len(),
        "开始原子写 session 文件"
    );
    #[cfg(unix)]
    let result = crate::hardening::atomic_write_private(path, content);
    #[cfg(not(unix))]
    let result: anyhow::Result<()> = Err(anyhow::anyhow!("session storage unavailable"));
    result.map_err(|_| {
        tracing::debug!(
            event = "session_atomic_write_failed",
            "原子写 session 文件失败"
        );
        SessionError::Io
    })?;
    tracing::debug!(
        event = "session_atomic_write_committed",
        bytes = content.len(),
        "原子写 session 文件已提交"
    );
    Ok(())
}

/// 检查单个 JSON 值的容器嵌套深度，并拒绝早期 syntax/pathological input。
fn validate_json_depth(bytes: &[u8]) -> Result<(), SessionError> {
    let mut stack = Vec::new();
    let mut in_string = false;
    let mut escaped = false;
    for byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match *byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                stack.push(*byte);
                if stack.len() > MAX_JSON_DEPTH {
                    tracing::debug!(event = "session_json_depth_limit", "JSON 深度超过上限");
                    return Err(SessionError::Corrupt);
                }
            }
            b'}' => {
                if stack.pop() != Some(b'{') {
                    return Err(SessionError::Corrupt);
                }
            }
            b']' => {
                if stack.pop() != Some(b'[') {
                    return Err(SessionError::Corrupt);
                }
            }
            _ => {}
        }
    }
    if in_string || escaped || !stack.is_empty() {
        return Err(SessionError::Corrupt);
    }
    Ok(())
}

/// 解析 records.jsonl，严格执行单行、记录数、schema 和 sequence 限制。
fn parse_records(bytes: &[u8]) -> Result<Vec<SessionRecord>, SessionError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    let mut previous_sequence = None;
    for raw_segment in bytes.split_inclusive(|byte| *byte == b'\n') {
        let mut line = raw_segment;
        if line.last() == Some(&b'\n') {
            line = &line[..line.len() - 1];
        }
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if line.is_empty() || line.len() > MAX_LINE_BYTES {
            tracing::debug!(
                event = "session_record_line_invalid",
                "journal 行为空或超出上限"
            );
            return Err(SessionError::Corrupt);
        }
        validate_json_depth(line)?;
        let wire: PersistedRecord = match serde_json::from_slice(line) {
            Ok(wire) => wire,
            Err(_) => {
                tracing::debug!(
                    event = "session_record_json_invalid",
                    "journal record JSON 解析失败"
                );
                return Err(SessionError::Corrupt);
            }
        };
        let record = match SessionRecord::try_from(wire) {
            Ok(record) => record,
            Err(_) => {
                tracing::debug!(
                    event = "session_record_schema_invalid",
                    "journal record schema 校验失败"
                );
                return Err(SessionError::Corrupt);
            }
        };
        if let Some(previous) = previous_sequence
            && record.sequence() <= previous
        {
            tracing::debug!(
                event = "session_record_sequence_invalid",
                "journal sequence 非单调"
            );
            return Err(SessionError::Corrupt);
        }
        previous_sequence = Some(record.sequence());
        records.push(record);
        if records.len() > MAX_RECORDS {
            tracing::debug!(event = "session_record_count_limit", "journal 记录超过上限");
            return Err(SessionError::Corrupt);
        }
    }
    if !bytes.ends_with(b"\n") {
        let last_start = match bytes.iter().rposition(|byte| *byte == b'\n') {
            Some(index) => index + 1,
            None => 0,
        };
        let line = &bytes[last_start..];
        if line.is_empty() || line.len() > MAX_LINE_BYTES {
            return Err(SessionError::Corrupt);
        }
    }
    Ok(records)
}

/// 校验单条 record 的字段闭集、必填标识和可序列化大小。
///
/// 工具 name 在这里只按历史审计 identifier 校验，不能把存储成功误当成可进入模型、
/// MCP call 或 replay；这些安全入口必须另行使用 contract qualified-name gate。
fn validate_record(record: &SessionRecord) -> Result<(), SessionError> {
    let prompt_id = record.prompt_id().ok_or(SessionError::InvalidRecord)?;
    if !is_prompt_id(prompt_id) {
        return Err(SessionError::InvalidRecord);
    }
    match record {
        SessionRecord::User { text, .. } => validate_text_size(text),
        SessionRecord::AssistantSnapshot { block_id, text, .. } => {
            validate_identifier(block_id)?;
            validate_text_size(text)
        }
        SessionRecord::AssistantToolCalls {
            tool_calls, text, ..
        } => {
            if tool_calls.is_empty() || tool_calls.len() > 128 {
                return Err(SessionError::InvalidRecord);
            }
            let mut ids = BTreeSet::new();
            for call in tool_calls {
                validate_identifier(&call.tool_call_id)?;
                validate_identifier(&call.name)?;
                if !ids.insert(&call.tool_call_id) {
                    return Err(SessionError::InvalidRecord);
                }
            }
            validate_text_size(text)
        }
        SessionRecord::Tool {
            tool_call_id,
            name,
            detail,
            status,
            ..
        } => {
            validate_identifier(tool_call_id)?;
            validate_identifier(name)?;
            validate_identifier(status)?;
            validate_text_size(detail)
        }
        SessionRecord::TurnTerminal { status, .. } => validate_identifier(status),
        SessionRecord::CompactSummary { text, .. } => {
            if text.trim().is_empty() {
                return Err(SessionError::InvalidRecord);
            }
            validate_text_size(text)
        }
    }
}

/// 校验公开标识字段不携带控制字符且不会无限增长。
fn validate_identifier(value: &str) -> Result<(), SessionError> {
    if value.is_empty() || value.len() > MAX_RECORD_ID_BYTES || value.chars().any(char::is_control)
    {
        Err(SessionError::InvalidRecord)
    } else {
        Ok(())
    }
}

/// 预估文本字段上限，最终 JSON 行长度仍由调用方再次校验。
fn validate_text_size(value: &str) -> Result<(), SessionError> {
    if value.len() > MAX_LINE_BYTES {
        Err(SessionError::InvalidRecord)
    } else {
        Ok(())
    }
}

/// 把目录读取错误收敛成不泄露路径的稳定分类。
fn map_directory_error(error: io::Error, missing_is_not_found: bool) -> SessionError {
    if missing_is_not_found && error.kind() == io::ErrorKind::NotFound {
        SessionError::NotFound
    } else {
        SessionError::Io
    }
}

/// 旧目录候选；`cwd_component` 只用于校验 summary 身份，不进入 v1 文件。
#[derive(Debug, Clone)]
struct LegacyCandidate {
    path: PathBuf,
    session_id: String,
    cwd_component: String,
    duplicate: bool,
}

/// 从旧 summary 提取的最小元数据，不反序列化完整旧 shell 类型。
#[derive(Debug, Clone)]
struct LegacySummaryMetadata {
    session_id: String,
    cwd: String,
    chat_format_version: u8,
    title: Option<String>,
    read_only: bool,
}

/// legacy 解析结果；只包含可安全展示或继续所需的白名单字段。
#[derive(Debug, Default)]
struct LegacyParseResult {
    records: Vec<SessionRecord>,
    thinking: Vec<ThinkingSnapshot>,
    read_only: bool,
    legacy_unknown_total: usize,
    legacy_control_total: usize,
    legacy_corrupt_total: usize,
    partial_tail: bool,
}

/// updates journal 中当前仍未闭合的 turn。
#[derive(Debug, Clone)]
struct LegacyActiveTurn {
    prompt_id: String,
    terminal: bool,
    has_user: bool,
    has_assistant: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyUpdateKind {
    User,
    Assistant,
    Thought,
    Tool,
    ToolUpdate,
    Terminal,
    LastTurnSummary,
    Ignore,
    Control,
}

/// 读取旧 summary 的最小安全字段；未知业务字段由旧格式演进自行保留。
fn read_legacy_summary(
    session_dir: &Path,
    expected_session_id: &str,
) -> Result<LegacySummaryMetadata, SessionError> {
    let bytes = read_legacy_file(&session_dir.join(LEGACY_SUMMARY_FILE))?;
    validate_json_depth(&bytes)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
        tracing::debug!(
            event = "legacy_summary_parse_failed",
            "legacy summary JSON 解析失败"
        );
        SessionError::Corrupt
    })?;
    let root = value.as_object().ok_or(SessionError::Corrupt)?;
    let info = root
        .get("info")
        .and_then(serde_json::Value::as_object)
        .ok_or(SessionError::Corrupt)?;
    let summary_id = info
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionError::Corrupt)?
        .to_owned();
    validate_legacy_metadata_string(&summary_id, MAX_LEGACY_METADATA_ID_BYTES)?;
    let cwd = info
        .get("cwd")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionError::Corrupt)?
        .to_owned();
    validate_legacy_metadata_string(&cwd, MAX_LINE_BYTES)?;

    let chat_format_version = match root.get("chat_format_version") {
        None => 0,
        Some(value) => value
            .as_u64()
            .and_then(|version| u8::try_from(version).ok())
            .ok_or(SessionError::Corrupt)?,
    };
    if !matches!(chat_format_version, 0 | 1) {
        tracing::debug!(
            event = "legacy_chat_format_unknown",
            "拒绝未知 legacy chat format version"
        );
        return Err(SessionError::Corrupt);
    }

    let session_summary = root
        .get("session_summary")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionError::Corrupt)?
        .to_owned();
    validate_legacy_metadata_string(&session_summary, MAX_LINE_BYTES)?;
    let generated_title = optional_legacy_string(root, "generated_title")?;
    if let Some(title) = &generated_title {
        validate_legacy_metadata_string(title, MAX_LINE_BYTES)?;
    }
    if let Some(title_is_manual) = root.get("title_is_manual")
        && !title_is_manual.is_boolean()
    {
        return Err(SessionError::Corrupt);
    }
    let title = generated_title
        .filter(|title| !title.trim().is_empty())
        .or_else(|| {
            (!session_summary.trim().is_empty()).then(|| session_summary.trim().to_owned())
        });

    Ok(LegacySummaryMetadata {
        session_id: summary_id.clone(),
        cwd,
        chat_format_version,
        title,
        read_only: summary_id != expected_session_id,
    })
}

/// 获取允许为空或 null 的旧 summary 字符串字段。
fn optional_legacy_string(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>, SessionError> {
    match object.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .map(str::to_owned)
            .ok_or(SessionError::Corrupt)
            .map(Some),
    }
}

/// 校验旧元数据字符串，避免控制字符进入 list 或导入 manifest。
fn validate_legacy_metadata_string(value: &str, max_bytes: usize) -> Result<(), SessionError> {
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        Err(SessionError::Corrupt)
    } else {
        Ok(())
    }
}

/// 旧 shell 的短 cwd 使用 URL percent encoding；无法解码的长 slug 不可继续。
fn decode_legacy_cwd(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return None;
            }
            let high = hex_value(bytes[index + 1])?;
            let low = hex_value(bytes[index + 2])?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    let value = String::from_utf8(decoded).ok()?;
    (value.starts_with('/') || (cfg!(windows) && value.as_bytes().get(1) == Some(&b':')))
        .then_some(value)
}

/// 解析 percent encoding 的十六进制半字节。
fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

/// 从旧 cwd 目录恢复可核对的原始路径；长路径使用 `.cwd` 元数据文件。
fn legacy_candidate_cwd(candidate: &LegacyCandidate) -> Result<Option<String>, SessionError> {
    if let Some(cwd) = decode_legacy_cwd(&candidate.cwd_component) {
        return Ok(Some(cwd));
    }
    let cwd_dir = candidate.path.parent().ok_or(SessionError::Corrupt)?;
    let cwd_file = cwd_dir.join(".cwd");
    let bytes = match fs::symlink_metadata(&cwd_file) {
        Ok(_) => read_legacy_file(&cwd_file)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(SessionError::Io),
    };
    let cwd = String::from_utf8(bytes)
        .map_err(|_| SessionError::Corrupt)?
        .trim()
        .to_owned();
    if cwd.is_empty()
        || !(cwd.starts_with('/') || (cfg!(windows) && cwd.as_bytes().get(1) == Some(&b':')))
    {
        return Err(SessionError::Corrupt);
    }
    validate_legacy_metadata_string(&cwd, MAX_LINE_BYTES)?;
    Ok(Some(cwd))
}

/// summary 与目录位置必须一致，否则只能作为只读历史展示。
fn legacy_summary_matches_candidate(
    candidate: &LegacyCandidate,
    summary: &LegacySummaryMetadata,
) -> Result<bool, SessionError> {
    Ok(summary.session_id == candidate.session_id
        && legacy_candidate_cwd(candidate)?.is_some_and(|cwd| cwd == summary.cwd))
}

/// 读取旧格式文件：允许旧 shell 的 0644 文件，但仍拒绝 symlink、硬链接和超限。
fn read_legacy_file(path: &Path) -> Result<Vec<u8>, SessionError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            SessionError::Corrupt
        } else {
            SessionError::Io
        }
    })?;
    verify_legacy_file_metadata(&metadata)?;
    #[cfg(unix)]
    {
        use std::io::Read as _;
        use std::os::unix::fs::OpenOptionsExt;
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| SessionError::Io)?;
        let opened_metadata = file.metadata().map_err(|_| SessionError::Io)?;
        verify_legacy_file_metadata(&opened_metadata)?;
        let mut bytes = Vec::new();
        file.take(MAX_SESSION_FILE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SessionError::Io)?;
        if bytes.len() > MAX_SESSION_FILE_BYTES {
            return Err(SessionError::Corrupt);
        }
        return Ok(bytes);
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(SessionError::Io)
    }
}

/// 校验旧格式文件的常规文件类型、单 inode 和固定大小上限。
fn verify_legacy_file_metadata(metadata: &Metadata) -> Result<(), SessionError> {
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(SessionError::Corrupt);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(SessionError::Corrupt);
        }
    }
    if metadata.len() > MAX_SESSION_FILE_BYTES as u64 {
        return Err(SessionError::Corrupt);
    }
    Ok(())
}

/// 判断 legacy 固定文件是否存在；存在但类型不安全时不把它当作缺失。
fn legacy_file_exists(session_dir: &Path, file_name: &str) -> Result<bool, SessionError> {
    match fs::symlink_metadata(session_dir.join(file_name)) {
        Ok(metadata) => {
            verify_legacy_file_metadata(&metadata)?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(SessionError::Io),
    }
}

fn legacy_summary_file_exists(session_dir: &Path) -> Result<bool, SessionError> {
    legacy_file_exists(session_dir, LEGACY_SUMMARY_FILE)
}

/// 旧目录只要求每一级是真目录；其最终 v1 目录仍由严格 0700 校验保护。
fn verify_legacy_directory(path: &Path) -> Result<(), SessionError> {
    validate_home_path(path)?;
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                SessionError::NotFound
            } else {
                SessionError::Io
            }
        })?;
        verify_directory_metadata(&metadata)?;
    }
    Ok(())
}

/// 只打开已有 legacy sessions 根，避免读取时创建目录或修改旧文件。
fn existing_legacy_sessions_root(repository_home: &Path) -> Result<Option<PathBuf>, SessionError> {
    match verify_directory(repository_home) {
        Ok(()) => {}
        Err(SessionError::NotFound) => return Ok(None),
        Err(error) => return Err(error),
    }
    let sessions_root = repository_home.join(LEGACY_SESSIONS_ROOT);
    match verify_legacy_directory(&sessions_root) {
        Ok(()) => Ok(Some(sessions_root)),
        Err(SessionError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

/// 将旧 JSONL 分割为受单行和总记录数限制的借用切片。
fn legacy_lines<'a>(bytes: &'a [u8]) -> Result<Vec<(usize, &'a [u8])>, SessionError> {
    let mut lines = Vec::new();
    for (line_index, segment) in bytes.split_inclusive(|byte| *byte == b'\n').enumerate() {
        let mut line = segment;
        if line.last() == Some(&b'\n') {
            line = &line[..line.len() - 1];
        }
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if line.len() > MAX_LINE_BYTES {
            return Err(SessionError::Corrupt);
        }
        if line.is_empty() || line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        if lines.len() >= MAX_RECORDS {
            return Err(SessionError::Corrupt);
        }
        lines.push((line_index, line));
    }
    Ok(lines)
}

/// 解析单行 JSON，不在日志中输出原文。
fn parse_legacy_json_line(line: &[u8]) -> Result<serde_json::Value, SessionError> {
    validate_json_depth(line)?;
    serde_json::from_slice(line).map_err(|_| SessionError::Corrupt)
}

/// 在已完成 turn 后允许跳过损坏行；损坏 active turn 必须整体 fail-closed。
fn recover_legacy_corrupt_line(
    result: &mut LegacyParseResult,
    active: Option<&LegacyActiveTurn>,
    line_index: usize,
) -> Result<(), SessionError> {
    if active.is_some_and(|turn| !turn.terminal) {
        tracing::debug!(
            event = "legacy_corrupt_active_turn",
            line_index,
            "legacy active turn 含损坏记录"
        );
        return Err(SessionError::Corrupt);
    }
    result.legacy_corrupt_total = result.legacy_corrupt_total.saturating_add(1);
    result.read_only = true;
    tracing::debug!(
        event = "legacy_corrupt_line_skipped",
        line_index,
        "跳过已完成边界后的 legacy 损坏行"
    );
    Ok(())
}

/// 将白名单记录加入导入结果并复用 v1 的字段校验。
fn append_legacy_record(
    result: &mut LegacyParseResult,
    record: SessionRecord,
) -> Result<(), SessionError> {
    if result.records.len() >= MAX_RECORDS {
        return Err(SessionError::Corrupt);
    }
    validate_record(&record).map_err(|_| SessionError::Corrupt)?;
    result.records.push(record);
    Ok(())
}

/// 更新 active turn，禁止把错误 prompt 边界当成可继续上下文。
fn activate_legacy_turn(
    active: &mut Option<LegacyActiveTurn>,
    prompt_id: &str,
    is_user: bool,
    result: &mut LegacyParseResult,
) {
    if let Some(current) = active.as_ref() {
        if current.terminal && !is_user {
            result.read_only = true;
        } else if !current.terminal && current.prompt_id != prompt_id {
            result.read_only = true;
        }
    }
    let should_start = active
        .as_ref()
        .is_none_or(|current| current.terminal || current.prompt_id != prompt_id);
    if should_start {
        *active = Some(LegacyActiveTurn {
            prompt_id: prompt_id.to_owned(),
            terminal: false,
            has_user: is_user,
            has_assistant: false,
        });
    } else if is_user && let Some(current) = active.as_mut() {
        current.has_user = true;
    }
}

/// 从旧 ACP envelope 提取 prompt id；缺失时只生成展示用 id。
fn legacy_prompt_id(
    params: &serde_json::Map<String, serde_json::Value>,
    update: &serde_json::Map<String, serde_json::Value>,
    session_id: &str,
    record_index: usize,
    terminal: bool,
) -> Result<(String, bool), SessionError> {
    if let Some(meta) = params.get("_meta")
        && !meta.is_null()
        && !meta.is_object()
    {
        return Err(SessionError::Corrupt);
    }
    if let Some(prompt_id) = params
        .get("_meta")
        .and_then(serde_json::Value::as_object)
        .and_then(|meta| meta.get("promptId"))
    {
        let prompt_id = prompt_id.as_str().ok_or(SessionError::Corrupt)?;
        if !prompt_id.is_empty() {
            validate_legacy_prompt_id(prompt_id)?;
            return Ok((prompt_id.to_owned(), true));
        }
    }
    if terminal {
        if let Some(prompt_id) = update.get("prompt_id") {
            let prompt_id = prompt_id.as_str().ok_or(SessionError::Corrupt)?;
            if !prompt_id.is_empty() {
                validate_legacy_prompt_id(prompt_id)?;
                return Ok((prompt_id.to_owned(), true));
            }
        }
    }
    Ok((format!("legacy:{session_id}:{record_index}"), false))
}

/// 校验原始或派生 prompt id 的持久化边界。
fn validate_legacy_prompt_id(prompt_id: &str) -> Result<(), SessionError> {
    if is_prompt_id(prompt_id) {
        Ok(())
    } else {
        Err(SessionError::Corrupt)
    }
}

/// 校验 envelope 的 sessionId；缺失字段只能进入只读状态。
fn observe_legacy_session_id(
    params: &serde_json::Map<String, serde_json::Value>,
    summary: &LegacySummaryMetadata,
    result: &mut LegacyParseResult,
) -> Result<(), SessionError> {
    match params.get("sessionId") {
        Some(value) => {
            let observed = value.as_str().ok_or(SessionError::Corrupt)?;
            if observed != summary.session_id {
                result.read_only = true;
            }
        }
        None => result.read_only = true,
    }
    Ok(())
}

/// 获取 ACP text content；非 text 内容不猜测、不写入 v1。
fn legacy_update_text(
    update: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, SessionError> {
    let content = update
        .get("content")
        .and_then(serde_json::Value::as_object)
        .ok_or(SessionError::Corrupt)?;
    if content.get("type").and_then(serde_json::Value::as_str) != Some("text") {
        return Err(SessionError::Corrupt);
    }
    let text = content
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionError::Corrupt)?;
    validate_text_size(text)?;
    Ok(text.to_owned())
}

/// 从 ACP tool update 读取 camelCase/snake_case 兼容 id。
fn legacy_tool_call_id(
    update: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, SessionError> {
    let value = update
        .get("toolCallId")
        .or_else(|| update.get("tool_call_id"))
        .ok_or(SessionError::Corrupt)?;
    let id = value.as_str().ok_or(SessionError::Corrupt)?;
    validate_identifier(id).map_err(|_| SessionError::Corrupt)?;
    Ok(id.to_owned())
}

/// 只提取 xAI 公开工具名，不把 title、rawInput 或结果写入 v1。
///
/// 这里保留通用 identifier 解析以兼容旧审计值；`LegacyToolPolicy` 决定该值是否
/// 具备继续资格，模型 transcript、MCP call 与 replay 入口还会再次 fail-closed。
fn legacy_qualified_tool_name(
    update: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<String>, SessionError> {
    let update_meta = match update.get("_meta") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(value.as_object().ok_or(SessionError::Corrupt)?),
    };
    let tool_meta = update_meta.and_then(|meta| meta.get("x.ai/tool"));
    let qualified = tool_meta
        .and_then(serde_json::Value::as_object)
        .and_then(|meta| meta.get("name"))
        .or_else(|| update.get("name"));
    let Some(value) = qualified else {
        return Ok(None);
    };
    let name = value.as_str().ok_or(SessionError::Corrupt)?;
    validate_identifier(name).map_err(|_| SessionError::Corrupt)?;
    Ok(Some(name.to_owned()))
}

/// 只允许更新已有工具摘要的状态字段，并报告是否找到对应调用。
fn update_legacy_tool_status(
    result: &mut LegacyParseResult,
    tool_call_id: &str,
    status: Option<&str>,
) -> Result<bool, SessionError> {
    if let Some(status) = status {
        validate_identifier(status).map_err(|_| SessionError::Corrupt)?;
    }
    for record in result.records.iter_mut().rev() {
        if let SessionRecord::Tool {
            tool_call_id: current_id,
            status: current_status,
            ..
        } = record
            && current_id == tool_call_id
        {
            if let Some(status) = status {
                *current_status = status.to_owned();
            }
            return Ok(true);
        }
    }
    Ok(false)
}

/// 映射旧 xAI/ACP update tag；未列出的 tag 一律拒绝猜测。
fn legacy_update_kind(tag: &str) -> Option<LegacyUpdateKind> {
    match tag {
        "user_message_chunk" => Some(LegacyUpdateKind::User),
        "agent_message_chunk" => Some(LegacyUpdateKind::Assistant),
        "agent_thought_chunk" => Some(LegacyUpdateKind::Thought),
        "tool_call" => Some(LegacyUpdateKind::Tool),
        "tool_call_update" => Some(LegacyUpdateKind::ToolUpdate),
        "turn_completed" => Some(LegacyUpdateKind::Terminal),
        "last_turn_summary" => Some(LegacyUpdateKind::LastTurnSummary),
        "compaction_checkpoint"
        | "rewind_marker"
        | "rewind_point"
        | "replace_chat_history"
        | "replace_history"
        | "plan"
        | "plan_state"
        | "workflow"
        | "workflow_updated"
        | "goal_updated"
        | "auto_compact_started"
        | "auto_compact_completed"
        | "auto_compact_failed"
        | "auto_compact_cancelled"
        | "auto_continue_completed"
        | "memory_flush_started"
        | "memory_flush_completed"
        | "memory_dream_completed"
        | "memory_session_saved"
        | "subagent_spawned"
        | "subagent_progress"
        | "subagent_finished" => Some(LegacyUpdateKind::Control),
        "available_commands_update"
        | "current_mode_update"
        | "config_option_update"
        | "session_info_update"
        | "usage_update"
        | "diff_review"
        | "retry_state"
        | "feedback_request"
        | "relay_sync_status"
        | "auto_recovery_started"
        | "auto_recovery_exhausted"
        | "hook_annotation"
        | "hook_execution"
        | "hooks_changed"
        | "plugins_changed"
        | "plugin_updates_installed"
        | "session_summary_generated"
        | "session_recap"
        | "session_recap_unavailable"
        | "task_completed"
        | "task_backgrounded"
        | "scheduled_task_created"
        | "scheduled_task_fired"
        | "scheduled_task_deleted"
        | "monitor_event"
        | "model_auto_switched"
        | "model_changed"
        | "tool_call_delta_chunk"
        | "image_compressed"
        | "image_dropped"
        | "memory_files"
        | "pending_interaction"
        | "interaction_resolved"
        | "response_started"
        | "reasoning_completed"
        | "response_completed"
        | "generated_title"
        | "manual_title_renamed" => Some(LegacyUpdateKind::Ignore),
        _ => None,
    }
}

/// 从 updates.jsonl 映射最小 transcript，并在边界不安全时只读或失败。
fn parse_legacy_updates(
    session_dir: &Path,
    session_id: &str,
    summary: &LegacySummaryMetadata,
    policy: &LegacyToolPolicy,
) -> Result<LegacyParseResult, SessionError> {
    let bytes = read_legacy_file(&session_dir.join(LEGACY_UPDATES_FILE))?;
    let lines = legacy_lines(&bytes)?;
    let mut result = LegacyParseResult::default();
    let mut active = None;
    if lines.is_empty() {
        result.read_only = true;
        return Ok(result);
    }

    for (record_index, line) in lines {
        let value = match parse_legacy_json_line(line) {
            Ok(value) => value,
            Err(_) => {
                recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                continue;
            }
        };
        let root = match value.as_object() {
            Some(root) => root,
            None => {
                recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                continue;
            }
        };
        let params = match root.get("method") {
            Some(method) => {
                let method = match method.as_str() {
                    Some(method) => method,
                    None => {
                        recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                        continue;
                    }
                };
                if !matches!(method, LEGACY_ACP_UPDATE_METHOD | LEGACY_XAI_UPDATE_METHOD) {
                    tracing::debug!(
                        event = "legacy_update_method_unknown",
                        "拒绝未知 legacy method"
                    );
                    return Err(SessionError::Corrupt);
                }
                match root.get("params").and_then(serde_json::Value::as_object) {
                    Some(params) => params,
                    None => {
                        recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                        continue;
                    }
                }
            }
            None => root,
        };
        let update = match params.get("update").and_then(serde_json::Value::as_object) {
            Some(update) => update,
            None => {
                recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                continue;
            }
        };
        observe_legacy_session_id(params, summary, &mut result)?;
        let tag = match update
            .get("sessionUpdate")
            .and_then(serde_json::Value::as_str)
        {
            Some(tag) => tag,
            None => {
                recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                continue;
            }
        };
        let kind = legacy_update_kind(tag).ok_or_else(|| {
            tracing::debug!(
                event = "legacy_update_tag_unknown",
                "拒绝未知 legacy update tag"
            );
            SessionError::Corrupt
        })?;
        if matches!(
            kind,
            LegacyUpdateKind::LastTurnSummary | LegacyUpdateKind::Ignore
        ) {
            continue;
        }
        if matches!(kind, LegacyUpdateKind::Control) {
            result.legacy_control_total = result.legacy_control_total.saturating_add(1);
            result.read_only = true;
            continue;
        }

        let (prompt_id, original_prompt_id) = legacy_prompt_id(
            params,
            update,
            session_id,
            record_index,
            matches!(kind, LegacyUpdateKind::Terminal),
        )
        .or_else(|_| {
            recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)
                .map(|()| (String::new(), false))
        })?;
        if prompt_id.is_empty() {
            continue;
        }
        if !original_prompt_id {
            result.read_only = true;
        }

        match kind {
            LegacyUpdateKind::User => {
                let text = match legacy_update_text(update) {
                    Ok(text) => text,
                    Err(_) => {
                        recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                        continue;
                    }
                };
                activate_legacy_turn(&mut active, &prompt_id, true, &mut result);
                let sequence = result.records.len() as u64;
                append_legacy_record(&mut result, SessionRecord::user(sequence, prompt_id, text))?;
            }
            LegacyUpdateKind::Assistant => {
                let text = match legacy_update_text(update) {
                    Ok(text) => text,
                    Err(_) => {
                        recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                        continue;
                    }
                };
                let had_active = active.is_some();
                activate_legacy_turn(&mut active, &prompt_id, false, &mut result);
                if !had_active {
                    result.read_only = true;
                }
                if let Some(turn) = active.as_mut() {
                    turn.has_assistant = true;
                }
                let block_id = update
                    .get("messageId")
                    .or_else(|| update.get("message_id"))
                    .map(|value| value.as_str().ok_or(SessionError::Corrupt))
                    .transpose()?
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("legacy-block-{record_index}"));
                validate_identifier(&block_id).map_err(|_| SessionError::Corrupt)?;
                let sequence = result.records.len() as u64;
                append_legacy_record(
                    &mut result,
                    SessionRecord::assistant_snapshot(sequence, prompt_id, block_id, text, false),
                )?;
            }
            LegacyUpdateKind::Thought => {
                let text = match legacy_update_text(update) {
                    Ok(text) => text,
                    Err(_) => {
                        recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                        continue;
                    }
                };
                result.thinking.push(ThinkingSnapshot {
                    sequence: record_index as u64,
                    prompt_id,
                    text,
                });
            }
            LegacyUpdateKind::Tool => {
                let tool_call_id = match legacy_tool_call_id(update) {
                    Ok(id) => id,
                    Err(_) => {
                        recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                        continue;
                    }
                };
                let qualified_name = match legacy_qualified_tool_name(update) {
                    Ok(name) => name,
                    Err(_) => {
                        recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                        continue;
                    }
                };
                if qualified_name
                    .as_deref()
                    .is_none_or(|name| !policy.allows(name))
                {
                    result.read_only = true;
                }
                let name = qualified_name.unwrap_or_else(|| {
                    result.read_only = true;
                    "legacy_tool".to_owned()
                });
                let had_active = active.is_some();
                activate_legacy_turn(&mut active, &prompt_id, false, &mut result);
                if !had_active {
                    result.read_only = true;
                }
                let sequence = result.records.len() as u64;
                append_legacy_record(
                    &mut result,
                    SessionRecord::tool(
                        sequence,
                        prompt_id,
                        tool_call_id,
                        name,
                        "legacy tool call",
                        "in_progress",
                    ),
                )?;
            }
            LegacyUpdateKind::ToolUpdate => {
                let tool_call_id = match legacy_tool_call_id(update) {
                    Ok(id) => id,
                    Err(_) => {
                        recover_legacy_corrupt_line(&mut result, active.as_ref(), record_index)?;
                        continue;
                    }
                };
                let status = update
                    .get("status")
                    .map(|value| value.as_str().ok_or(SessionError::Corrupt))
                    .transpose()?;
                let had_active = active.is_some();
                activate_legacy_turn(&mut active, &prompt_id, false, &mut result);
                if !had_active {
                    result.read_only = true;
                }
                if !update_legacy_tool_status(&mut result, &tool_call_id, status)? {
                    // 没有初始 tool_call 时无法恢复完整工具上下文，只读保全边界。
                    result.read_only = true;
                }
            }
            LegacyUpdateKind::Terminal => {
                let stop_reason = update
                    .get("stop_reason")
                    .or_else(|| update.get("stopReason"))
                    .and_then(serde_json::Value::as_str)
                    .ok_or(SessionError::Corrupt)?;
                validate_identifier(stop_reason).map_err(|_| SessionError::Corrupt)?;
                let had_active = active.is_some();
                activate_legacy_turn(&mut active, &prompt_id, false, &mut result);
                if !had_active {
                    result.read_only = true;
                }
                if let Some(turn) = active.as_mut() {
                    if !turn.has_user {
                        result.read_only = true;
                    }
                    turn.terminal = true;
                }
                let sequence = result.records.len() as u64;
                append_legacy_record(
                    &mut result,
                    SessionRecord::turn_terminal(sequence, prompt_id, stop_reason),
                )?;
            }
            LegacyUpdateKind::LastTurnSummary
            | LegacyUpdateKind::Ignore
            | LegacyUpdateKind::Control => unreachable!("legacy kind handled above"),
        }
    }
    if active.as_ref().is_none_or(|turn| !turn.terminal) {
        result.read_only = true;
        result.partial_tail = active.as_ref().is_some_and(|turn| turn.has_assistant);
    }
    if result.records.is_empty() && result.thinking.is_empty() {
        result.read_only = true;
    }
    Ok(result)
}

/// 从 version 0/1 chat_history.jsonl 提取可展示文本；不恢复未知 payload。
fn parse_legacy_chat_history(
    session_dir: &Path,
    session_id: &str,
    summary: &LegacySummaryMetadata,
) -> Result<LegacyParseResult, SessionError> {
    let bytes = read_legacy_file(&session_dir.join(LEGACY_CHAT_HISTORY_FILE))?;
    let lines = legacy_lines(&bytes)?;
    let mut result = LegacyParseResult {
        read_only: true,
        ..LegacyParseResult::default()
    };
    for (record_index, line) in lines {
        let value = parse_legacy_json_line(line)?;
        let object = value.as_object().ok_or(SessionError::Corrupt)?;
        if summary.chat_format_version == 0 {
            parse_legacy_chat_message_v0(&mut result, object, session_id, record_index)?;
        } else {
            parse_legacy_conversation_item_v1(&mut result, object, session_id, record_index)?;
        }
    }
    Ok(result)
}

/// 解析旧 ChatRequestMessage 的 user/assistant 白名单字段。
fn parse_legacy_chat_message_v0(
    result: &mut LegacyParseResult,
    object: &serde_json::Map<String, serde_json::Value>,
    session_id: &str,
    record_index: usize,
) -> Result<(), SessionError> {
    let role = object
        .get("role")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionError::Corrupt)?;
    match role {
        "user" | "assistant" => {
            let (text, unsupported_content) =
                legacy_chat_content(object.get("content").ok_or(SessionError::Corrupt)?)?;
            if unsupported_content {
                result.legacy_unknown_total = result.legacy_unknown_total.saturating_add(1);
                result.read_only = true;
            }
            let prompt_id = format!("legacy:{session_id}:{record_index}");
            let sequence = result.records.len() as u64;
            if role == "user" {
                append_legacy_record(result, SessionRecord::user(sequence, prompt_id, text))?;
            } else {
                if let Some(reasoning) = object.get("reasoning_content") {
                    let reasoning = reasoning.as_str().ok_or(SessionError::Corrupt)?;
                    if !reasoning.is_empty() {
                        validate_text_size(reasoning)?;
                        result.thinking.push(ThinkingSnapshot {
                            sequence: record_index as u64,
                            prompt_id: prompt_id.clone(),
                            text: reasoning.to_owned(),
                        });
                    }
                }
                if let Some(tool_calls) = object.get("tool_calls") {
                    let calls = tool_calls.as_array().ok_or(SessionError::Corrupt)?;
                    if !calls.is_empty() {
                        result.legacy_unknown_total = result.legacy_unknown_total.saturating_add(1);
                        result.read_only = true;
                    }
                }
                append_legacy_record(
                    result,
                    SessionRecord::assistant_snapshot(
                        sequence,
                        prompt_id.clone(),
                        format!("legacy-block-{record_index}"),
                        text,
                        false,
                    ),
                )?;
                let terminal_sequence = result.records.len() as u64;
                append_legacy_record(
                    result,
                    SessionRecord::turn_terminal(terminal_sequence, prompt_id, "completed"),
                )?;
            }
        }
        "system" | "tool" => {
            result.legacy_unknown_total = result.legacy_unknown_total.saturating_add(1);
            result.read_only = true;
        }
        _ => return Err(SessionError::Corrupt),
    }
    Ok(())
}

/// 解析 version 1 ConversationItem，仅展示 user/assistant，其余只计数。
fn parse_legacy_conversation_item_v1(
    result: &mut LegacyParseResult,
    object: &serde_json::Map<String, serde_json::Value>,
    session_id: &str,
    record_index: usize,
) -> Result<(), SessionError> {
    let item_type = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionError::Corrupt)?;
    match item_type {
        "user" | "assistant" => {
            let (text, unsupported_content) =
                legacy_chat_content(object.get("content").ok_or(SessionError::Corrupt)?)?;
            if unsupported_content {
                result.legacy_unknown_total = result.legacy_unknown_total.saturating_add(1);
            }
            let prompt_id = format!("legacy:{session_id}:{record_index}");
            let sequence = result.records.len() as u64;
            if item_type == "user" {
                append_legacy_record(result, SessionRecord::user(sequence, prompt_id, text))?;
            } else {
                if object
                    .get("tool_calls")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|calls| !calls.is_empty())
                {
                    result.legacy_unknown_total = result.legacy_unknown_total.saturating_add(1);
                }
                append_legacy_record(
                    result,
                    SessionRecord::assistant_snapshot(
                        sequence,
                        prompt_id.clone(),
                        format!("legacy-block-{record_index}"),
                        text,
                        false,
                    ),
                )?;
                let terminal_sequence = result.records.len() as u64;
                append_legacy_record(
                    result,
                    SessionRecord::turn_terminal(terminal_sequence, prompt_id, "completed"),
                )?;
            }
        }
        "system" | "tool_result" | "tool" | "auth" | "reasoning" | "backend_tool_call" => {
            result.legacy_unknown_total = result.legacy_unknown_total.saturating_add(1);
            result.read_only = true;
        }
        _ => return Err(SessionError::Corrupt),
    }
    Ok(())
}

/// 解析旧消息 content 的字符串或 text block；image 等已知但不可映射内容只计数。
fn legacy_chat_content(value: &serde_json::Value) -> Result<(String, bool), SessionError> {
    if let Some(text) = value.as_str() {
        validate_text_size(text)?;
        return Ok((text.to_owned(), false));
    }
    let blocks = value.as_array().ok_or(SessionError::Corrupt)?;
    let mut text = String::new();
    let mut unsupported = false;
    for block in blocks {
        let block = block.as_object().ok_or(SessionError::Corrupt)?;
        match block.get("type").and_then(serde_json::Value::as_str) {
            Some("text") => {
                let part = block
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(SessionError::Corrupt)?;
                text.push_str(part);
            }
            Some("image") | Some("image_url") => unsupported = true,
            _ => return Err(SessionError::Corrupt),
        }
    }
    validate_text_size(&text)?;
    Ok((text, unsupported))
}

/// legacy 读取成功后用于原子目录发布的目录同步。
fn sync_directory(path: &Path) -> Result<(), SessionError> {
    let directory = fs::File::open(path).map_err(|_| SessionError::Io)?;
    directory.sync_all().map_err(|_| SessionError::Io)
}

/// 将白名单 records 编码成 v1 JSONL，防止导入阶段绕过 Task 15 限制。
fn encode_records(records: &[SessionRecord]) -> Result<Vec<u8>, SessionError> {
    if records.len() > MAX_RECORDS {
        return Err(SessionError::Corrupt);
    }
    let mut output = Vec::new();
    let mut previous_sequence = None;
    for record in records {
        if let Some(previous) = previous_sequence
            && record.sequence() <= previous
        {
            return Err(SessionError::Corrupt);
        }
        let wire = PersistedRecord::try_from(record).map_err(|_| SessionError::Corrupt)?;
        let line = serde_json::to_vec(&wire).map_err(|_| SessionError::Corrupt)?;
        if line.len() > MAX_LINE_BYTES {
            return Err(SessionError::Corrupt);
        }
        output.extend_from_slice(&line);
        output.push(b'\n');
        if output.len() > MAX_SESSION_FILE_BYTES {
            return Err(SessionError::Corrupt);
        }
        previous_sequence = Some(record.sequence());
    }
    Ok(output)
}

/// manifest 的闭集 JSON 结构。
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    session_id: String,
    /// 首次 legacy 导入后保留的展示标题。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    /// legacy 元数据或不完整 turn 导致的只读状态。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    read_only: bool,
    /// 只展示、不送入模型的 thinking 快照。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    thinking: Vec<ThinkingSnapshot>,
    /// 未映射 legacy item 的累计数量。
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    legacy_unknown_total: usize,
    /// legacy 控制事件的累计数量。
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    legacy_control_total: usize,
    /// 可恢复边界外被忽略的损坏行数量。
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    legacy_corrupt_total: usize,
    /// 最后 assistant 没有 terminal 的 partial tail。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    partial_tail: bool,
}

impl Manifest {
    /// 从内存快照构造 v1 manifest，不把 runtime 或 legacy 原始 payload 写盘。
    fn from_session(session: &Session) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            session_id: session.id.clone(),
            title: session.title.clone(),
            read_only: session.read_only,
            thinking: session.thinking.clone(),
            legacy_unknown_total: session.legacy_unknown_total,
            legacy_control_total: session.legacy_control_total,
            legacy_corrupt_total: session.legacy_corrupt_total,
            partial_tail: session.partial_tail,
        }
    }
}

/// `usize` 计数字段的 serde skip predicate。
fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

/// 校验 v1 manifest 的扩展字段，保证旧版最小 manifest 仍可读取。
fn validate_session_metadata(manifest: &Manifest) -> Result<(), SessionError> {
    validate_session_id(&manifest.session_id).map_err(|_| SessionError::Corrupt)?;
    if let Some(title) = &manifest.title {
        validate_legacy_metadata_string(title, MAX_LINE_BYTES)?;
    }
    if manifest.thinking.len() > MAX_RECORDS {
        return Err(SessionError::Corrupt);
    }
    if manifest.legacy_unknown_total > MAX_RECORDS
        || manifest.legacy_control_total > MAX_RECORDS
        || manifest.legacy_corrupt_total > MAX_RECORDS
    {
        return Err(SessionError::Corrupt);
    }
    let mut previous_sequence = None;
    for thinking in &manifest.thinking {
        validate_legacy_prompt_id(&thinking.prompt_id)?;
        validate_text_size(&thinking.text).map_err(|_| SessionError::Corrupt)?;
        if let Some(previous) = previous_sequence
            && thinking.sequence <= previous
        {
            return Err(SessionError::Corrupt);
        }
        previous_sequence = Some(thinking.sequence);
    }
    Ok(())
}

/// 不含任意工具参数的持久化调用元数据。
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedToolCall {
    tool_call_id: String,
    name: String,
}

/// journal 的闭集 wire 结构；schema_version 位于每条记录内部。
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PersistedRecord {
    /// 用户记录 wire。
    User {
        schema_version: u32,
        sequence: u64,
        prompt_id: String,
        text: String,
    },
    /// assistant 快照 wire。
    AssistantSnapshot {
        schema_version: u32,
        sequence: u64,
        prompt_id: String,
        block_id: String,
        text: String,
        streaming: bool,
    },
    /// assistant 工具 round wire；历史参数永远不落盘。
    AssistantToolCalls {
        schema_version: u32,
        sequence: u64,
        prompt_id: String,
        round: u32,
        tool_calls: Vec<PersistedToolCall>,
        text: String,
    },
    /// 工具摘要 wire。
    Tool {
        schema_version: u32,
        sequence: u64,
        prompt_id: String,
        #[serde(default)]
        round: u32,
        tool_call_id: String,
        name: String,
        detail: String,
        status: String,
    },
    /// turn 终态 wire。
    TurnTerminal {
        schema_version: u32,
        sequence: u64,
        prompt_id: String,
        status: String,
    },
    /// 模型上下文压缩摘要 wire。
    CompactSummary {
        schema_version: u32,
        sequence: u64,
        prompt_id: String,
        covered_until_sequence: u64,
        text: String,
    },
}

impl TryFrom<&SessionRecord> for PersistedRecord {
    type Error = SessionError;

    fn try_from(record: &SessionRecord) -> Result<Self, Self::Error> {
        validate_record(record)?;
        Ok(match record {
            SessionRecord::User {
                sequence,
                prompt_id,
                text,
            } => Self::User {
                schema_version: SCHEMA_VERSION,
                sequence: *sequence,
                prompt_id: prompt_id.clone(),
                text: text.clone(),
            },
            SessionRecord::AssistantSnapshot {
                sequence,
                prompt_id,
                block_id,
                text,
                streaming,
            } => Self::AssistantSnapshot {
                schema_version: SCHEMA_VERSION,
                sequence: *sequence,
                prompt_id: prompt_id.clone(),
                block_id: block_id.clone(),
                text: text.clone(),
                streaming: *streaming,
            },
            SessionRecord::AssistantToolCalls {
                sequence,
                prompt_id,
                round,
                tool_calls,
                text,
            } => Self::AssistantToolCalls {
                schema_version: SCHEMA_VERSION,
                sequence: *sequence,
                prompt_id: prompt_id.clone(),
                round: *round,
                tool_calls: tool_calls
                    .iter()
                    .map(|call| PersistedToolCall {
                        tool_call_id: call.tool_call_id.clone(),
                        name: call.name.clone(),
                    })
                    .collect(),
                text: text.clone(),
            },
            SessionRecord::Tool {
                sequence,
                prompt_id,
                round,
                tool_call_id,
                name,
                detail,
                status,
            } => Self::Tool {
                schema_version: SCHEMA_VERSION,
                sequence: *sequence,
                prompt_id: prompt_id.clone(),
                round: *round,
                tool_call_id: tool_call_id.clone(),
                name: name.clone(),
                detail: detail.clone(),
                status: status.clone(),
            },
            SessionRecord::TurnTerminal {
                sequence,
                prompt_id,
                status,
            } => Self::TurnTerminal {
                schema_version: SCHEMA_VERSION,
                sequence: *sequence,
                prompt_id: prompt_id.clone(),
                status: status.clone(),
            },
            SessionRecord::CompactSummary {
                sequence,
                prompt_id,
                covered_until_sequence,
                text,
            } => Self::CompactSummary {
                schema_version: SCHEMA_VERSION,
                sequence: *sequence,
                prompt_id: prompt_id.clone(),
                covered_until_sequence: *covered_until_sequence,
                text: text.clone(),
            },
        })
    }
}

impl TryFrom<PersistedRecord> for SessionRecord {
    type Error = SessionError;

    fn try_from(record: PersistedRecord) -> Result<Self, Self::Error> {
        let result = match record {
            PersistedRecord::User {
                schema_version,
                sequence,
                prompt_id,
                text,
            } => {
                if schema_version != SCHEMA_VERSION {
                    return Err(SessionError::Corrupt);
                }
                Self::User {
                    sequence,
                    prompt_id,
                    text,
                }
            }
            PersistedRecord::AssistantSnapshot {
                schema_version,
                sequence,
                prompt_id,
                block_id,
                text,
                streaming,
            } => {
                if schema_version != SCHEMA_VERSION {
                    return Err(SessionError::Corrupt);
                }
                Self::AssistantSnapshot {
                    sequence,
                    prompt_id,
                    block_id,
                    text,
                    streaming,
                }
            }
            PersistedRecord::AssistantToolCalls {
                schema_version,
                sequence,
                prompt_id,
                round,
                tool_calls,
                text,
            } => {
                if schema_version != SCHEMA_VERSION {
                    return Err(SessionError::Corrupt);
                }
                Self::AssistantToolCalls {
                    sequence,
                    prompt_id,
                    round,
                    tool_calls: tool_calls
                        .into_iter()
                        .map(|call| ToolCallSnapshot {
                            tool_call_id: call.tool_call_id,
                            name: call.name,
                        })
                        .collect(),
                    text,
                }
            }
            PersistedRecord::Tool {
                schema_version,
                sequence,
                prompt_id,
                round,
                tool_call_id,
                name,
                detail,
                status,
            } => {
                if schema_version != SCHEMA_VERSION {
                    return Err(SessionError::Corrupt);
                }
                Self::Tool {
                    sequence,
                    prompt_id,
                    round,
                    tool_call_id,
                    name,
                    detail,
                    status,
                }
            }
            PersistedRecord::TurnTerminal {
                schema_version,
                sequence,
                prompt_id,
                status,
            } => {
                if schema_version != SCHEMA_VERSION {
                    return Err(SessionError::Corrupt);
                }
                Self::TurnTerminal {
                    sequence,
                    prompt_id,
                    status,
                }
            }
            PersistedRecord::CompactSummary {
                schema_version,
                sequence,
                prompt_id,
                covered_until_sequence,
                text,
            } => {
                if schema_version != SCHEMA_VERSION {
                    return Err(SessionError::Corrupt);
                }
                Self::CompactSummary {
                    sequence,
                    prompt_id,
                    covered_until_sequence,
                    text,
                }
            }
        };
        validate_record(&result).map_err(|_| SessionError::Corrupt)?;
        Ok(result)
    }
}

impl Serialize for SessionRecord {
    /// 公开序列化也使用与 journal 相同的 schema 白名单。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        PersistedRecord::try_from(self)
            .map_err(|_| <S::Error as serde::ser::Error>::custom("invalid session record"))?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SessionRecord {
    /// 公开反序列化拒绝未知字段、错误版本和不安全标识。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PersistedRecord::deserialize(deserializer)?;
        Self::try_from(wire)
            .map_err(|_| <D::Error as serde::de::Error>::custom("invalid session record"))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_JSON_DEPTH, SessionError, SessionRecord, SessionRepository, validate_json_depth,
        validate_session_id,
    };

    #[test]
    fn session_id_contract_is_ascii_bounded() {
        assert!(validate_session_id("session-1").is_ok());
        assert!(validate_session_id("../escape").is_err());
        assert!(validate_session_id(&"a".repeat(129)).is_err());
    }

    #[test]
    fn json_depth_contract_rejects_nested_values() {
        let nested = (0..=MAX_JSON_DEPTH).fold("null".to_owned(), |value, _| format!("[{value}]"));
        assert!(validate_json_depth(nested.as_bytes()).is_err());
    }

    #[test]
    fn record_helpers_preserve_prompt_and_sequence() {
        let record = SessionRecord::user(4, "prompt", "text");
        assert_eq!(record.prompt_id(), Some("prompt"));
        assert_eq!(record.sequence(), 4);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_removes_created_v1_session() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("创建 session 存储临时目录");
        let home = temporary.path().join("home");
        fs::create_dir(&home).expect("创建 repository home");
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).expect("设置 home 权限");
        let repository = SessionRepository::new(&home);
        let session = repository.create().await.expect("创建 v1 session");
        repository
            .delete(&session.id)
            .await
            .expect("删除刚创建的 session");
        let listed = repository.list().await.expect("列出剩余 session");
        assert!(listed.iter().all(|item| item.id != session.id));
        assert!(matches!(
            repository.delete(&session.id).await,
            Err(SessionError::NotFound)
        ));
    }
}
