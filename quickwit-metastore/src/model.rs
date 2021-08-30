use crate::schema::{indexes, splits};

#[derive(Identifiable, Insertable, Queryable, Debug)]
#[primary_key(index_id)]
#[table_name = "indexes"]
pub struct Index {
    pub index_id: String,
    pub index_uri: String,
    pub index_config: String,
    pub checkpoint: String,
}

#[derive(Identifiable, Insertable, Associations, Queryable, Debug)]
#[belongs_to(Index)]
#[primary_key(split_id)]
#[table_name = "splits"]
pub struct Split {
    pub split_id: String,
    pub split_state: i32,
    pub num_records: i64,
    pub size_in_bytes: i64,
    pub start_time_range: Option<i64>,
    pub end_time_range: Option<i64>,
    pub generation: i64,
    pub update_timestamp: i64,
    pub tags: String,
    pub start_footer_offset: i64,
    pub end_footer_offset: i64,
    pub index_id: String,
}
