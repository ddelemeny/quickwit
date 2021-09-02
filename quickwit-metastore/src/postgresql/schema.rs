table! {
    indexes (index_id) {
        index_id -> Varchar,
        index_uri -> Varchar,
        index_config -> Text,
        checkpoint -> Text,
    }
}

table! {
    splits (split_id) {
        split_id -> Varchar,
        split_state -> Varchar,
        num_records -> Int8,
        size_in_bytes -> Int8,
        start_time_range -> Nullable<Int8>,
        end_time_range -> Nullable<Int8>,
        generation -> Int8,
        update_timestamp -> Int8,
        tags -> Text,
        start_footer_offset -> Int8,
        end_footer_offset -> Int8,
        index_id -> Varchar,
    }
}

joinable!(splits -> indexes (index_id));

allow_tables_to_appear_in_same_query!(
    indexes,
    splits,
);
