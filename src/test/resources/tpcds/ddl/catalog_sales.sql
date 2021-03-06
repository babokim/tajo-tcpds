create external table if not exists ${DB}.catalog_sales
(
    cs_sold_date_sk           int4,
    cs_sold_time_sk           int4,
    cs_ship_date_sk           int4,
    cs_bill_customer_sk       int4,
    cs_bill_cdemo_sk          int4,
    cs_bill_hdemo_sk          int4,
    cs_bill_addr_sk           int4,
    cs_ship_customer_sk       int4,
    cs_ship_cdemo_sk          int4,
    cs_ship_hdemo_sk          int4,
    cs_ship_addr_sk           int4,
    cs_call_center_sk         int4,
    cs_catalog_page_sk        int4,
    cs_ship_mode_sk           int4,
    cs_warehouse_sk           int4,
    cs_item_sk                int4,
    cs_promo_sk               int4,
    cs_order_number           int4,
    cs_quantity               int4,
    cs_wholesale_cost         float4,
    cs_list_price             float4,
    cs_sales_price            float4,
    cs_ext_discount_amt       float4,
    cs_ext_sales_price        float4,
    cs_ext_wholesale_cost     float4,
    cs_ext_list_price         float4,
    cs_ext_tax                float4,
    cs_coupon_amt             float4,
    cs_ext_ship_cost          float4,
    cs_net_paid               float4,
    cs_net_paid_inc_tax       float4,
    cs_net_paid_inc_ship      float4,
    cs_net_paid_inc_ship_tax  float4,
    cs_net_profit             float4
)
using csv with ('text.delimiter'='|') location '${DATA_LOCATION}';
