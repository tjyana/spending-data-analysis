-- join credit card statements with expense tracker data

select *
from {{ ref("stg_expense_tracker__transactions_complete") }}

union all 

select * 
from {{ ref("int_transactions__credit_card_enriched")}}

union all

select *
from {{ ref("int_transactions__thb_cash")}}