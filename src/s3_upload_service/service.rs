use aws_sdk_s3::{primitives::ByteStream, Client};
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc::{self, Receiver, Sender}};

pub struct S3UploadServicePayload {
    file_path: String,
    total_files: u32,
    job_id: String
}

pub struct S3UploadService {
    sender: Sender<S3UploadServicePayload>,
    receiver: Receiver<S3UploadServicePayload>,
    s3_client: Client
}

pub enum S3UploadError {
    Failure(String)
}

impl S3UploadService {

    pub fn new(s3_client: Client) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        S3UploadService { sender,  receiver, s3_client }
    }

    pub fn get_sender(&self) -> Sender<S3UploadServicePayload> {
        self.sender.clone()
    }

    pub async fn run(&mut self) {
        while let Some(val) = self.receiver.recv().await {
            unimplemented!();
        }
    }

    async fn upload_to_s3 (
        &self,
        file_path: String,
        bucket_name: &str
    ) -> Result<(), S3UploadError> {

        let mut file = File::open(&file_path)
            .await
            .map_err(|e| S3UploadError::Failure(e.to_string()))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .await
            .map_err(|e| S3UploadError::Failure(e.to_string()))?;
        let byte_stream = ByteStream::from(buffer);

        let key = std::path::Path::new(&file_path)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();

        self.s3_client
            .put_object()
            .bucket(bucket_name)
            .key(key)
            .body(byte_stream)
            .send()
            .await
            .map_err(|e| S3UploadError::Failure(e.to_string()))?;

        Ok(())
    }

    async fn process(&self) {
        //get the file_path, upload it to s3, send the file_path, job_id and status to another service on
        //success and failure. The other service will count success and failure against a job_id.



    }
}
