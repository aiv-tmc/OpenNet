use super::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use anyhow::Result;

pub struct TcpAdapter {
    listener: TcpListener,
    streams: Arc<Mutex<HashMap<String, TcpStream>>>,
    local_addr: std::net::SocketAddr,
}

impl TcpAdapter {
    pub async fn new() -> Result<Self> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let local_addr = listener.local_addr()?;
        Ok(Self {
            listener,
            streams: Arc::new(Mutex::new(HashMap::new())),
            local_addr,
        })
    }
    
    async fn connect(&self, addr: &str) -> Result<()> {
        let stream = TcpStream::connect(addr).await?;
        let id = format!("tcp:{}", stream.peer_addr()?);
        self.streams.lock().await.insert(id, stream);
        Ok(())
    }
}

#[async_trait]
impl NetworkAdapter for TcpAdapter {
    async fn send(&self, dest: &str, data: Bytes) -> Result<()> {
        let mut streams = self.streams.lock().await;
        if let Some(stream) = streams.get_mut(dest) {
            let len = data.len() as u16;
            stream.write_all(&len.to_be_bytes()).await?;
            stream.write_all(&data).await?;
        } else {
            drop(streams);
            self.connect(dest).await?;
            let mut streams = self.streams.lock().await;
            let stream = streams.get_mut(dest).unwrap();
            let len = data.len() as u16;
            stream.write_all(&len.to_be_bytes()).await?;
            stream.write_all(&data).await?;
        }
        Ok(())
    }
    
    async fn recv(&mut self) -> Result<(String, Bytes)> {
        let (mut stream, addr) = self.listener.accept().await?;   // <-- mut добавлен
        let id = format!("tcp:{}", addr);
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await?;
        let len = u16::from_be_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await?;
        self.streams.lock().await.insert(id.clone(), stream);
        Ok((id, Bytes::from(data)))
    }
    
    fn local_id(&self) -> String {
        format!("tcp:{}", self.local_addr)
    }
    
    fn priority(&self) -> u8 { 1 }
}
