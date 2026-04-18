use std::fmt::Display;

use serde::{Deserialize, Serialize};

pub enum SplitServiceError {
    Failed,
    FetchFailed(String),
    InvalidResponse,
    IOError(String),
}

impl Display for SplitServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SplitServiceError::FetchFailed(e) => {
                write!(f, "Failed to fetch {}", e)
            },
            SplitServiceError::Failed => {
                write!(f, "Something went wrong")
            },
            SplitServiceError::InvalidResponse => {
                write!(f, "Invalid Response from queue")
            },
            SplitServiceError::IOError(e) => {
                write!(f, "IO Error {}", e)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    pub task_type: String,
    pub job_id:  String,
    pub file_url: String,
    pub retry_left: u32
}
