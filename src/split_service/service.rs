use std::{error::Error};
use lopdf::Document;

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

async fn download_file(base_dir: &str, file_url: String, job_id: &str) -> Result<Option<String>, SplitServiceError> {
    let file_path = format!("{}/{}_basefile", base_dir, job_id);
    let mut res = reqwest::get(file_url).await.map_err(|e| SplitServiceError::FetchFailed(e.to_string()))?;
    let mut dest = File::create(&file_path).await.map_err(|e| SplitServiceError::IOError(e.to_string()))?;
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
    Ok(Some(file_path))
}

async fn process(task: Task, base_dir: &str) -> Result<(), SplitServiceError> {
    //After successful processing, empty the files directory, but do not delete it.
    let file_path = match download_file(base_dir, task.file_url, &task.job_id).await {
        Ok(Some(val)) => val,
        Ok(None) => {
            return Err(SplitServiceError::InvalidResponse)
        },
        Err(e) => {
            return Err(e)
        }
    };

    //Split the files, save to directory then send to file uploader service which uploads to s3
    let doc = Document::load(&file_path).map_err(|_| SplitServiceError::FileNotFound)?;
    let pages = doc.get_pages();
    for (i, _) in pages.iter().enumerate() {
        let page_number = (i + 1) as u32;
        
        // Load the document again for each page extraction
        let mut doc = Document::load(&file_path).map_err(|_| SplitServiceError::FileNotFound)?;
        
        // Retain only the current page (1-indexed)
        let pages_to_delete: Vec<u32> = pages
            .keys()
            .filter(|&&p| p != page_number)
            .cloned()
            .collect();
        doc.delete_pages(&pages_to_delete);
        
        // Save the new document
        let output_name = format!("{}_page_{}.pdf", file_path, page_number);
        doc.save(output_name).map_err(|_| SplitServiceError::Failed)?;
        println!("Saved: page_{}.pdf", page_number);
    }


    //perform splits, get all splitted files and upload all to s3 at the same time, by sending to a
    //upload service which will upload to s3, the upload service will then send to a service which will count the success
    //and if even one fail then fail the job
    //and send back to queue with a reduced retry count.
    //If all success then create job in db with status ocr_enqueue_pending for each then go ahead and upload to job queue. On failure then update status to ocr_enqueue_failed. A bg worker will retry the failed tasks
    //Retry worker will fetch these records by claiming. by getting the rows through update query
    //so that only a single worker will get and other workers which were also queuing will not
    //get. Make sure to put a limit on the query response to make sure a single worker does not
    //retry everything

    Ok(())
}

