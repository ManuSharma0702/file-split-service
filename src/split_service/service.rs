use std::error::Error;

use reqwest::Client;

use crate::split_service::value::{SplitServiceError, Task};

pub async fn run() -> Result<(), Box<dyn Error>> {
    loop {
        match get_split_task().await {
            Ok(Some(val)) => {
                dbg!("Data");
                dbg!(&val);
                continue;
            },
            Ok(None) => {
                dbg!("No data");
                continue;
            },
            Err(_) => {
                eprintln!("error");
                continue;
            }
        }
    }
}

async fn get_split_task() -> Result<Option<Task>, SplitServiceError> {

    let client  = Client::new();

    let url = "http://127.0.0.1:8080/task?task_type=split&timeout=10";

    let res = client.get(url)
        .send()
        .await
        .map_err(|_| SplitServiceError::FetchFailed)?;

    if res.status().is_success() {
        let task: Option<Task> = res
            .json()
            .await
            .map_err(|_| SplitServiceError::InvalidResponse)?;
        Ok(task)
    } else {
        Err(SplitServiceError::FetchFailed)
    }

}

async fn process() -> Result<(), SplitServiceError> {
    Ok(())
}
