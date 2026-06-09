use super::*;
use crate::dht::Dht;
use tokio::task::JoinHandle;
use std::sync::Arc;
use tokio::sync::Mutex;
use anyhow::Result;

const MULTICAST_ADDR: &str = "239.255.0.1";
const MULTICAST_PORT: u16 = 9999;

pub struct Manager {
    adapters: Vec<Box<dyn NetworkAdapter>>,
    current: usize,
}

impl Manager {
    pub async fn new() -> Result<Self> {
        let mut adapters: Vec<Box<dyn NetworkAdapter>> = vec![];
        
        if let Ok(tcp) = tcp::TcpAdapter::new().await {
            adapters.push(Box::new(tcp));
        }
        
        if let Ok(lora) = lora::LoraAdapter::new().await {
            adapters.push(Box::new(lora));
        }
        
        adapters.sort_by_key(|a| a.priority());
        Ok(Self { adapters, current: 0 })
    }
    
    pub async fn send(&mut self, dest: &str, data: Bytes) -> Result<()> {
        for i in self.current..self.adapters.len() {
            if let Ok(()) = self.adapters[i].send(dest, data.clone()).await {
                self.current = i;
                return Ok(());
            }
        }
        anyhow::bail!("No working network adapter")
    }
    
    pub async fn recv(&mut self) -> Result<(String, Bytes)> {
        let mut futs = vec![];
        for adapter in &mut self.adapters {
            futs.push(adapter.recv());
        }
        let (result, _, _) = futures::future::select_all(futs).await;
        result
    }
    
    pub fn local_id(&self) -> String {
        if self.adapters.is_empty() {
            "none".to_string()
        } else {
            self.adapters[self.current].local_id()
        }
    }
}

pub async fn start_discovery(manager: Arc<Mutex<Manager>>, dht: Arc<Mutex<Dht>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        use socket2::{Socket, Domain, Type, Protocol};
        use std::net::SocketAddr;
        
        let addr: SocketAddr = format!("0.0.0.0:{}", MULTICAST_PORT).parse().unwrap();
        let socket = match Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to create socket: {}", e);
                return;
            }
        };
        if let Err(e) = socket.set_reuse_address(true) {
            eprintln!("Failed to set reuse_address: {}", e);
        }
        if let Err(e) = socket.bind(&addr.into()) {
            eprintln!("Failed to bind socket: {}", e);
            return;
        }
        let std_socket: std::net::UdpSocket = socket.into();
        let socket = match tokio::net::UdpSocket::from_std(std_socket) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to convert to tokio socket: {}", e);
                return;
            }
        };
        
        let multicast_ip: std::net::Ipv4Addr = MULTICAST_ADDR.parse().unwrap();
        if let Err(e) = socket.join_multicast_v4(multicast_ip, "0.0.0.0".parse().unwrap()) {
            eprintln!("Failed to join multicast group: {}", e);
            return;
        }
        
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        let mut buf = [0u8; 1024];
        let send_addr = format!("{}:{}", MULTICAST_ADDR, MULTICAST_PORT);
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mgr = manager.lock().await;
                    let local_id = mgr.local_id();
                    let msg = format!("HELLO {}", local_id);
                    let _ = socket.send_to(msg.as_bytes(), &send_addr).await;
                }
                recv_res = socket.recv_from(&mut buf) => {
                    if let Ok((len, _src)) = recv_res {
                        if let Ok(text) = std::str::from_utf8(&buf[..len]) {
                            if text.starts_with("HELLO ") {
                                let peer_id_str = text.trim_start_matches("HELLO ");
                                let mut dht_guard = dht.lock().await;
                                let mut id_bytes = [0u8; 32];
                                let hash = blake3::hash(peer_id_str.as_bytes());
                                id_bytes.copy_from_slice(hash.as_bytes());
                                dht_guard.insert(crate::dht::PeerInfo {
                                    id: id_bytes,
                                    addr: peer_id_str.to_string(),
                                });
                                println!("[Discovery] Discovered peer: {}", hex::encode(&id_bytes[..8]));
                            }
                        }
                    }
                }
            }
        }
    })
}
