//! Kit 事件的运输端口。

use anyhow::Result;

use crate::KitProductEvent;

/// 运输端口。Tauri、测试内存总线与未来非 Tauri 适配器分别实现本 trait。
pub trait KitEventSink: Send + Sync {
    /// 由具体产品适配器完成已校验事件的实际投递。
    ///
    /// 此方法仅实现运输细节；Host 代码必须通过 [`Self::emit`] 进入产品边界。
    fn emit_to_product(&self, ev: KitProductEvent) -> Result<()>;

    /// 在事件进入产品运输层前校验 Kit 回合与会话标识不变量。
    ///
    /// 入站 serde 保持宽容；只有此出站边界会拒绝不合法事件，且不会将其交给具体
    /// 产品适配器。
    fn emit(&self, ev: KitProductEvent) -> Result<()> {
        ev.validate()
            .map_err(|error| anyhow::anyhow!("Kit 产品事件未通过出站标识校验: {error:?}"))?;
        self.emit_to_product(ev)
    }
}
