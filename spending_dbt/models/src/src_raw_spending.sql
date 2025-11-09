select * 
from {{ source('raw_spending', 'expense_tracker_responses') }}
