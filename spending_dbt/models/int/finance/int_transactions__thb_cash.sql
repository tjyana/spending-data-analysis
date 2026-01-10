select 
  id_key,
  timestamp_datetime,
  transaction_date,
  transaction_month,
  transaction_month_year,
  transaction_day_of_week,
  is_weekend,
  amount, -- doublecheck
  transaction_type,
  income,
  expense,
  payment_method,
  payment_method_complete,
  payee,
  payee_standardized, -- fix later
  item, 
  category,
  category_standardized, -- fix later
  category_complete, -- fix later
  tags,
  tags_complete, -- fix later
  food_details,
  hobby_details,
  trip_details,
  social,
  social_complete, -- fix later
  store_type, 
  store_type_standardized, -- fix later
  store_type_complete, -- fix later
  purchase_channel,
  purchase_channel_complete, -- fix later
  essentiality,
  essentiality_complete, -- fix later
  recurrence_type,
  recurrence_type_complete, -- fix later
  value_rating,
  value_rating_complete, -- fix later
  notes,
  anomaly,
  source_system

  from {{ ref('stg_thb__202512bkk') }}
  where id_key is null 