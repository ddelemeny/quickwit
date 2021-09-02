CREATE TABLE splits (
    split_id VARCHAR(50) PRIMARY KEY, 
    split_state INTEGER NOT NULL, 
    num_records BIGINT NOT NULL, 
    size_in_bytes BIGINT NOT NULL, 
    start_time_range BIGINT,
    end_time_range BIGINT,
    generation BIGINT NOT NULL,
    update_timestamp BIGINT NOT NULL, 
    tags TEXT NOT NULL, 
    start_footer_offset BIGINT NOT NULL, 
    end_footer_offset BIGINT NOT NULL, 
    index_id VARCHAR(50) NOT NULL,
 
    FOREIGN KEY(index_id) REFERENCES indexes(index_id)
);
