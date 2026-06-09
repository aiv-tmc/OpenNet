use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Text { from: String, to: String, content: String },
    FileChunk { id: u32, offset: u64, data: Vec<u8> },
    ComputeTask { task_id: u32, code: String, input: Vec<u8> },
    ComputeResult { task_id: u32, output: Vec<u8> },
    Ping,
    Pong,
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
    
    pub fn decode(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}
