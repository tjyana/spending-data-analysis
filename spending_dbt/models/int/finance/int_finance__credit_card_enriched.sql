-- -- int_finance__credit_card_enriched.sql

with cc as (
  select *
  from {{ ref('stg_credit_card__statements_complete') }}
),

thb as (
  select *
  from {{ ref('stg_thb__202512bkk') }}  -- or whatever your THB stg ref is
)

select
  -- 1) identifiers / timestamps
  coalesce(thb.id_key, cc.id_key) as id_key,
  cc.timestamp_datetime as timestamp_datetime,
  cc.transaction_date as transaction_date,
  cc.transaction_month as transaction_month,
  cc.transaction_month_year as transaction_month_year,
  cc.transaction_day_of_week as transaction_day_of_week,
  cc.is_weekend as is_weekend,
  coalesce(cast(cc.amount as numeric), cast(thb.JPY as numeric)) as amount, -- doublecheck
  cc.transaction_type as transaction_type,
  cc.income as income,
  cc.expense as expense,
  cc.payment_method as payment_method,
  cc.payment_method_complete as payment_method_complete,
  coalesce(thb.payee, cc.payee) as payee,
  cc.payee_standardized as payee_standardized, -- fix later
  coalesce(thb.item, cc.item) as item, 
  coalesce(thb.category, cc.category) as category,
  cc.category_standardized as category_standardized, -- fix later
  cc.category_complete as category_complete, -- fix later
  coalesce(thb.tags, cc.tags) as tags,
  cc.tags_complete as tags_complete, -- fix later
  coalesce(thb.food_details, cc.food_details) as food_details,
  coalesce(thb.hobby_details, cc.hobby_details) as hobby_details,
  coalesce(thb.trip_details, cc.trip_details) as trip_details,
  coalesce(thb.social, cc.social) as social,
  cc.social_complete as social_complete, -- fix later
  coalesce(thb.store_type, cc.store_type) as store_type, 
  cc.store_type_standardized as store_type_standardized, -- fix later
  cc.store_type_complete as store_type_complete, -- fix later
  coalesce(thb.purchase_channel, cc.purchase_channel) as purchase_channel,
  cc.purchase_channel_complete as purchase_channel_complete, -- fix later
  coalesce(thb.essentiality, cc.essentiality) as essentiality,
  cc.essentiality_complete as essentiality_complete, -- fix later
  coalesce(thb.recurrence_type, cc.recurrence_type) as recurrence_type,
  cc.recurrence_type_complete as recurrence_type_complete, -- fix later
  coalesce(thb.value_rating, cc.value_rating) as value_rating,
  cc.value_rating_complete as value_rating_complete, -- fix later
  coalesce(thb.notes, cc.notes) as notes,
  coalesce(thb.anomaly, cc.anomaly) as anomaly,
  coalesce(thb.source_system, cc.source_system) as source_system


from cc
left join thb
  on cc.id_key = thb.id_key


