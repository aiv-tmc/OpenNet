use crate::network::Manager;
use crate::dht::Dht;
use crate::fragment::Fragmenter;
use crate::message::Message;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::io::{AsyncBufReadExt, BufReader};
use anyhow::Result;
use bytes::Bytes;

pub async fn run(manager: Arc<Mutex<Manager>>, dht: Arc<Mutex<Dht>>) -> Result<()> {
    let fragmenter = Fragmenter::new();
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    
    println!("P2P Node Terminal");
    println!("Commands:");
    println!("  /list               - show known peers");
    println!("  /myid               - show my peer ID and network address");
    println!("  /msg <peer_id> <text> - send text message");
    println!("  /sendfile <peer> <path>  (not implemented)");
    println!("  /comput <peer> <code>    (not implemented)");
    
    loop {
        line.clear();
        reader.read_line(&mut line).await?;
        let line = line.trim();
        if line.is_empty() { continue; }
        
        if line.starts_with('/') {
            let parts: Vec<&str> = line.split_whitespace().collect();
            match parts[0] {
                "/list" => {
                    let peers = dht.lock().await.all_peers();
                    if peers.is_empty() {
                        println!("No peers discovered yet.");
                    } else {
                        println!("Known peers:");
                        for p in peers {
                            println!("  {}", hex::encode(p.id));
                        }
                    }
                }
                "/myid" => {
                    let mgr = manager.lock().await;
                    let local_id = mgr.local_id();
                    let hash = blake3::hash(local_id.as_bytes());
                    println!("My peer_id: {}", hex::encode(hash.as_bytes()));
                    println!("My network address: {}", local_id);
                }
                "/msg" => {
                    if parts.len() < 3 {
                        println!("Usage: /msg <peer_id> <text>");
                        continue;
                    }
                    let peer_id = parts[1];
                    let text = parts[2..].join(" ");
                    let msg = Message::Text {
                        from: "me".to_string(),
                        to: peer_id.to_string(),
                        content: text,
                    };
                    let data = msg.encode();
                    let msg_id = rand::random();
                    let fragments = fragmenter.fragment(msg_id, &Bytes::from(data));
                    let mut mgr = manager.lock().await;
                    for frag in fragments {
                        // In real app, we need to map peer_id to network address via DHT.
                        // For now, we just send to peer_id as if it were address.
                        if let Err(e) = mgr.send(peer_id, frag).await {
                            eprintln!("Failed to send: {}", e);
                        }
                    }
                }
                "/sendfile" => {
                    println!("File transfer not implemented yet");
                }
                "/comput" => {
                    println!("Distributed computing not implemented yet");
                }
                _ => println!("Unknown command: {}", parts[0]),
            }
        } else {
            println!("Unknown input. Use / commands.");
        }
        
        // Receive incoming messages (non-blocking check)
        let mut mgr = manager.lock().await;
        if let Ok((_src, data)) = mgr.recv().await {
            if data.len() >= 8 {
                let header = data[0..8].try_into().unwrap();
                if let Some(complete) = fragmenter.defragment(&header, &data[8..]).await {
                    if let Some(msg) = Message::decode(&complete) {
                        match msg {
                            Message::Text { from, content, .. } => {
                                println!("\n[MSG from {}] {}", from, content);
                            }
                            _ => println!("Received other message type"),
                        }
                    }
                }
            }
        }
    }
}
