select 
    *
    -- case when transaction_type = '💰income💰' then amount else 0 end as income,
    -- case when transaction_type = 'expense' then amount else 0 end as expense
from {{ ref('src_raw_spending') }}

-- separate out the expenses and income into plus minus in another column
-- when transaction_type is 'income', the amount is positive
-- when transaction_type is 'expense', the amount is negative