table! {
    indexes (index_id) {
        index_id -> Varchar,
        json -> Text,
    }
}

table! {
    splits (split_id) {
        split_id -> Varchar,
        split_state -> Varchar,
        start_time_range -> Nullable<Int8>,
        end_time_range -> Nullable<Int8>,
        tags -> Text,
        json ->Text,
        index_id -> Varchar,
    }
}

joinable!(splits -> indexes (index_id));

allow_tables_to_appear_in_same_query!(indexes, splits,);
