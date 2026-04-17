use serde::{Deserialize, Serialize};

pub enum SplitServiceError {
    Failed,
    FetchFailed,
    InvalidResponse
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    pub task_type: String,
    pub job_id:  String,
    pub file_url: String,
    pub retry_left: u32
}
