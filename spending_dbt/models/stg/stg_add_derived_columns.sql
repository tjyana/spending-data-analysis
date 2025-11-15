    -- case when transaction_type = '💰income💰' then amount else 0 end as income,
    -- case when transaction_type = 'expense' then amount else 0 end as expense
    -- add date, month and day of week, is weekend columns



with 

src as (
    select * 
    from {{ ref('src_raw_spending') }}
), 

income_expense as (
    select 
        timestamp_datetime,
        amount,
        case when transaction_type = '💰income💰' then amount else 0 end as income,
        case when transaction_type = 'expense' then -amount else 0 end as expense,
        payment_method,
        payee,
        item,
        category,
        tags,
        food_details,
        hobby_details,
        trip_details,
        social,
        store_type,
        purchase_channel,
        essentiality,
        recurrence_type,
        value_rating,
        notes
    from src
)

select * from income_expense
