-- join credit card statements with expense tracker data

with final as (

select *
from {{ ref("stg_expense_tracker__transactions_complete") }}

union all

select *
from {{ ref("int_transactions__credit_card_enriched")}}

union all

select *
from {{ ref("int_transactions__thb_cash")}}

)

select
  id_key,
  timestamp_datetime,
  transaction_date,
  transaction_month,
  transaction_month_year,
  transaction_day_of_week,
  is_weekend,
  amount,
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

from final
