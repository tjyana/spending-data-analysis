-- join credit card statements with expense tracker data

select *
from {{ ref("stg_expense_tracker__transactions_complete") }}

union all 

select * 
from {{ ref("stg_credit_card__statements_complete")}}