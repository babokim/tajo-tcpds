create external table if not exists ${DB}.store
(
    s_store_sk                int4,
    s_store_id                text,
    s_rec_start_date          date,
    s_rec_end_date            date,
    s_closed_date_sk          int4,
    s_store_name              text,
    s_number_employees        int4,
    s_floor_space             int4,
    s_hours                   text,
    s_manager                 text,
    s_market_id               int4,
    s_geography_class         text,
    s_market_desc             text,
    s_market_manager          text,
    s_division_id             int4,
    s_division_name           text,
    s_company_id              int4,
    s_company_name            text,
    s_street_number           text,
    s_street_name             text,
    s_street_type             text,
    s_suite_number            text,
    s_city                    text,
    s_county                  text,
    s_state                   text,
    s_zip                     text,
    s_country                 text,
    s_gmt_offset              float4,
    s_tax_precentage          float4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';