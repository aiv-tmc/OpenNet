mod tcp;
mod bluetooth;
mod lora;
mod manager;

pub use manager::Manager;
pub use manager::start_discovery;

use async_trait::async_trait;
use bytes::Bytes;
use anyhow::Result;

#[async_trait]
pub trait NetworkAdapter: Send + Sync {
    async fn send(&self, dest: &str, data: Bytes) -> Result<()>;
    async fn recv(&mut self) -> Result<(String, Bytes)>;
    fn local_id(&self) -> String;
    fn priority(&self) -> u8;
}
