use core::task;
use std::{error::Error};

use reqwest::Client;
use tokio::{fs::{self, File}, io::AsyncWriteExt};

use crate::split_service::value::{SplitServiceError, Task};

pub async fn run() -> Result<(), Box<dyn Error>> {
    //On init create a tmp directory for holding files.
    //Base file downloaded from s3 which will be split into pages and then each page will 
    //be uploaded to s3 in a directory by file_name and for each page a new task will be 
    //pushed to job queue for OCR with task having job_id, file_url (page), page_number, retry_left

    let base_dir = format!("{}/files", env!("CARGO_MANIFEST_DIR"));
    fs::create_dir_all(&base_dir).await?;

    loop {
        match get_split_task().await {
            Ok(Some(val)) => {
                if let Err(e) = process(val, &base_dir).await {
                    eprintln!("Error while splitting {}", e);
                }
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
        .map_err(|e| SplitServiceError::FetchFailed(e.to_string()))?;

    if res.status().is_success() {
        let task: Option<Task> = res
            .json()
            .await
            .map_err(|_| SplitServiceError::InvalidResponse)?;
        Ok(task)
    } else {
        Err(SplitServiceError::FetchFailed("INTERNAL SERVICE ERROR".to_string()))
    }
}

async fn download_file(base_dir: &str, file_url: String, job_id: &str) -> Result<(), SplitServiceError> {
    let file_path = format!("{}/{}_basefile", base_dir, job_id);
    let mut res = reqwest::get(file_url).await.map_err(|e| SplitServiceError::FetchFailed(e.to_string()))?;
    let mut dest = File::create(file_path).await.map_err(|e| SplitServiceError::IOError(e.to_string()))?;
    loop {
        match res.chunk().await {
            Ok(Some(chunk)) => {
                dest.write_all(&chunk).await.map_err(|e| SplitServiceError::IOError(e.to_string()))?;
            },
            Ok(None) => break,
            Err(e) => {
                return Err(SplitServiceError::FetchFailed(e.to_string()));
            }
        }
    }
    Ok(())
}

async fn process(task: Task, base_dir: &str) -> Result<(), SplitServiceError> {
    //After successful processing, empty the files directory, but do not delete it.
    download_file(base_dir, task.file_url, &task.job_id).await?;

    //perform splits, get all splitted files and upload all to s3 at the same time.

    //After successful upload of each file. get count of success counts if the success matches the total pages only then start creating task and pushing to job queue.
    //else fail the job and send back to job queue with reduced retry count.
    //If some passed and others failed then what should happen with already uploaded s3 file.
    //Should get replaced by another worker on retry.
    //after all file upload success create a task for each file page in db in table ocr_tasks with
    //job id as reference with status ocr_enqueue_pending.
    //then create a task for each and send to job queue. on failure update ocr_enqueue_failed. and a bg worker will retry these
    //failed status. 
    //Retyr worker will fetch these records by claiming. by getting the rows through update query
    //so that only a single worker will get and other workers which were also queuing will not
    //get. Make sure to put a limit on the queue response to make sure a single worker does not
    //retry everything.

    Ok(())
}

