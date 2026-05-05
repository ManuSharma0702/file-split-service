use std::{fmt::Display, str::FromStr};

use sqlx::{Pool, Postgres};
use uuid::Uuid;

#[derive(Debug)]
pub enum JobCreationError {
    Failed(String),
    DBError(String)
}

impl Display for JobCreationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobCreationError::DBError(e) => {
                write!(f, "Database error: {}", e)
            },
            JobCreationError::Failed(e) => {
                write!(f, "Job creation failed {}", e)
            }
        }
    }
}


pub async fn job_enqueue_fail(db_conn: &Pool<Postgres>, job_id: &str) -> Result<(), JobCreationError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| JobCreationError::DBError(e.to_string()))?;
    
    sqlx::query(
        r#"
        UPDATE jobs
        SET
            enqueue_left = GREATEST(enqueue_left - 1, 0),
            status = CASE
                WHEN enqueue_left - 1 <= 0 THEN 'dead'
                ELSE 'split_enqueue_failed'
            END
        WHERE id = $1
        "#
    )
    .bind(uuid)
    .execute(db_conn)
    .await
    .map_err(|e| JobCreationError::DBError(e.to_string()))?;

    Ok(())
}

