-- -- int_finance__credit_card_enriched.sql

with cc as (
  select *
  from {{ ref('stg_credit_card__statements_complete') }}
),

thb as (
  select *
  from {{ ref('stg_thb__202512bkk') }}  -- or whatever your THB stg ref is
),

et as (
  select *
  from {{ ref('stg_expense_tracker__transactions_complete')}}
)

select
  -- 1) identifiers / timestamps
  coalesce(thb.id_key, cc.id_key, et.id_key) as id_key,
  cc.timestamp_datetime as timestamp_datetime, -- processed in cc
  cc.transaction_date as transaction_date, -- processed in cc
  cc.transaction_month as transaction_month, -- processed in cc
  cc.transaction_month_year as transaction_month_year, -- processed in cc
  cc.transaction_day_of_week as transaction_day_of_week, -- processed in cc
  cc.is_weekend as is_weekend, -- processed in cc
  coalesce(cast(cc.amount as numeric), cast(thb.JPY as numeric), cast(et.amount as numeric)) as amount, -- we want the cc amount here
  cc.transaction_type as transaction_type, -- keep cc value
  cc.income as income, -- keep cc value
  cc.expense as expense, -- keep cc value
  coalesce(et.payment_method, thb.payment_method, cc.payment_method) as payment_method,
  coalesce(et.payment_method_complete, thb.payment_method, cc.payment_method_complete) as payment_method_complete,
  coalesce(et.payee, thb.payee, cc.payee) as payee,
  coalesce(et.payee_standardized, thb.payee, cc.payee_standardized) as payee_standardized,
  coalesce(et.item, thb.item, cc.item) as item, 
  coalesce(et.category, thb.category, cc.category) as category,
  coalesce(et.category_standardized, thb.category, cc.category_standardized) as category_standardized, 
  coalesce(et.category_complete, thb.category, cc.category_complete) as category_complete, 
  coalesce(et.tags, thb.tags, cc.tags) as tags,
  coalesce(et.tags_complete, thb.tags, cc.tags_complete) as tags_complete, 
  coalesce(et.food_details, thb.food_details, cc.food_details) as food_details,
  coalesce(et.hobby_details, thb.hobby_details, cc.hobby_details) as hobby_details,
  coalesce(et.trip_details, thb.trip_details, cc.trip_details) as trip_details,
  coalesce(et.social, thb.social, cc.social) as social,
  coalesce(et.social_complete, thb.social, cc.social_complete) as social_complete, 
  coalesce(et.store_type, thb.store_type, cc.store_type) as store_type, 
  coalesce(et.store_type_standardized, thb.store_type, cc.store_type_standardized) as store_type_standardized, 
  coalesce(et.store_type_complete, thb.store_type, cc.store_type_complete) as store_type_complete, 
  coalesce(et.purchase_channel, thb.purchase_channel, cc.purchase_channel) as purchase_channel,
  coalesce(et.purchase_channel_complete, thb.purchase_channel, cc.purchase_channel_complete) as purchase_channel_complete, 
  coalesce(et.essentiality, thb.essentiality, cc.essentiality) as essentiality,
  coalesce(et.essentiality_complete, thb.essentiality, cc.essentiality_complete) as essentiality_complete,
  coalesce(et.recurrence_type, thb.recurrence_type, cc.recurrence_type) as recurrence_type,
  coalesce(et.recurrence_type_complete, thb.recurrence_type, cc.recurrence_type_complete) as recurrence_type_complete, 
  coalesce(et.value_rating, thb.value_rating, cc.value_rating) as value_rating,
  coalesce(et.value_rating_complete, thb.value_rating, cc.value_rating_complete) as value_rating_complete, 
  coalesce(et.notes, thb.notes, cc.notes) as notes,
  coalesce(et.anomaly, thb.anomaly, cc.anomaly) as anomaly,
  coalesce(et.source_system, thb.source_system, cc.source_system) as source_system

from cc

left join thb
  on cc.id_key = thb.id_key
left join et 
  on cc.id_key = et.id_key


