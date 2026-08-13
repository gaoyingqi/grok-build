//! Kit 事件的运输端口。

use anyhow::Result;

use crate::KitProductEvent;

/// 运输端口。Tauri、测试内存总线与未来非 Tauri 适配器分别实现本 trait。
pub trait KitEventSink: Send + Sync {
    /// 将已经符合 Kit 协议的事件投递给产品运输层。
    fn emit(&self, ev: KitProductEvent) -> Result<()>;
}
