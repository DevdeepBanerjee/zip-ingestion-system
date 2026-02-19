CREATE TABLE IF NOT EXISTS file_metadata (
    id SERIAL PRIMARY KEY,
    upload_id VARCHAR(100),
    file_name TEXT,
    file_path TEXT,
    file_size BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (upload_id, file_name)
);
