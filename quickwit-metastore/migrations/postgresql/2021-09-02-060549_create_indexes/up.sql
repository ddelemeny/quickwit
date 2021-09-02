CREATE TABLE indexes ( 
    index_id VARCHAR(50) PRIMARY KEY, 
    index_uri VARCHAR(50) NOT NULL, 
    index_config TEXT NOT NULL,
    checkpoint TEXT NOT NULL
);
