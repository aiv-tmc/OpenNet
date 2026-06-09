use std::collections::HashMap;

#[derive(Clone)]
pub struct PeerInfo {
    pub id: [u8; 32],
    pub addr: String,
}

pub struct Dht {
    routing_table: HashMap<[u8; 32], PeerInfo>,
}

impl Dht {
    pub fn new() -> Self {
        Self { routing_table: HashMap::new() }
    }
    
    pub fn insert(&mut self, peer: PeerInfo) {
        self.routing_table.insert(peer.id, peer);
    }
    
    #[allow(dead_code)]
    pub fn find_node(&self, target_id: &[u8; 32]) -> Vec<PeerInfo> {
        let mut peers: Vec<_> = self.routing_table.values().cloned().collect();
        peers.sort_by_key(|p| xor_distance(&p.id, target_id));
        peers.into_iter().take(20).collect()
    }
    
    pub fn all_peers(&self) -> Vec<PeerInfo> {
        self.routing_table.values().cloned().collect()
    }
}

#[allow(dead_code)]
fn xor_distance(a: &[u8; 32], b: &[u8; 32]) -> u64 {
    let mut d = 0u64;
    for i in 0..8 {
        d |= ((a[i] ^ b[i]) as u64) << (i * 8);
    }
    d
}
