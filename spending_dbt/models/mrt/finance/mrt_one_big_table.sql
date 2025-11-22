select * 
from {{ ref('int_finance__transactions_union') }}