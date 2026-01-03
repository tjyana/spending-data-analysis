select 
  id_key,
  timestamp_datetime,
  date(timestamp_datetime) as transaction_date,
  format_datetime('%m', timestamp_datetime) as transaction_month,
  format_datetime('%m/%Y', timestamp_datetime) as transaction_month_year,
  format_datetime('%a', timestamp_datetime) as transaction_day_of_week,
  case 
    when regexp_contains(format_date('%a', timestamp_datetime), r'Sat|Sun') then 1
    else 0
  end as is_weekend,
  amount, 
  transaction_type,
  case 
    when transaction_type = '💰income💰' then amount 
    else 0 
  end as income,  
  case 
    when transaction_type = 'expense' or transaction_type is null then -amount 
    else 0 
  end as expense,
  payment_method,
  payment_method as payment_method_complete,
  payee,
  payee as payee_standardized, -- fix later
  item, 
  category,
  category as category_standardized, -- fix later
  category as category_complete, -- fix later
  tags,
  tags as tags_complete, -- fix later
  food_details,
  hobby_details,
  trip_details,
  social,
  social as social_complete, -- fix later
  store_type, 
  store_type as store_type_standardized, -- fix later
  store_type as store_type_complete, -- fix later
  purchase_channel,
  purchase_channel as purchase_channel_complete, -- fix later
  essentiality,
  essentiality as essentiality_complete, -- fix later
  recurrence_type,
  recurrence_type as recurrence_type_complete, -- fix later
  value_rating,
  value_rating as value_rating_complete, -- fix later
  notes,
  anomaly,
  source_system,
  JPY



from {{ ref('src_thb__202512bkk') }}