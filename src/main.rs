mod network;
mod dht;
mod fragment;
mod handshake;
mod terminal;
mod message;

use std::sync::Arc;
use tokio::sync::Mutex;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Init network manager with priority: WiFi(TCP) -> Bluetooth -> LoRa
    let net_mgr = Arc::new(Mutex::new(network::Manager::new().await?));
    
    // Init DHT (simplified Kademlia)
    let dht = Arc::new(Mutex::new(dht::Dht::new()));
    
    // Start discovery (mDNS, Bluetooth SDP, LoRa broadcast)
    let discovery_handle = network::start_discovery(net_mgr.clone(), dht.clone()).await;
    
    // Interactive terminal
    terminal::run(net_mgr, dht).await?;
    
    discovery_handle.abort();
    Ok(())
}
