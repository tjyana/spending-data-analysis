-- join credit card statements with expense tracker data

select *
from {{ ref("stg_expense_tracker__add_derived_columns") }}