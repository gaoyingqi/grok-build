//! Kit 事件的运输端口与 Host 出站验证包装器。

use anyhow::Result;

use crate::KitProductEvent;

/// 运输端口。Tauri、测试内存总线与未来非 Tauri 适配器分别实现本 trait。
pub trait KitEventSink: Send + Sync {
    /// 将事件投递给产品运输层。
    fn emit(&self, ev: KitProductEvent) -> Result<()>;
}

/// 在 Host 持有的产品运输端口外统一执行 Kit 出站标识校验。
///
/// 内部 sink 没有公开访问器，HostRuntime 在构造时将所有产品适配器包入此类型，
/// 使 Host 的每一条出站事件都必须先通过 [`KitProductEvent::validate`]。
pub struct ValidatedKitEventSink<S: KitEventSink> {
    sink: S,
}

impl<S: KitEventSink> ValidatedKitEventSink<S> {
    /// 用具体产品运输适配器构造不可绕过其内部 sink 的验证边界。
    pub fn new(sink: S) -> Self {
        Self { sink }
    }
}

impl<S: KitEventSink> KitEventSink for ValidatedKitEventSink<S> {
    /// 拒绝无效事件后不调用内部产品运输适配器。
    fn emit(&self, ev: KitProductEvent) -> Result<()> {
        ev.validate()
            .map_err(|error| anyhow::anyhow!("Kit 产品事件未通过出站标识校验: {error:?}"))?;
        self.sink.emit(ev)
    }
}
