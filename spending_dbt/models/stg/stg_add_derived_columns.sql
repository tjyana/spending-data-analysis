    -- case when transaction_type = '💰income💰' then amount else 0 end as income,
    -- case when transaction_type = 'expense' then amount else 0 end as expense
    -- add date, month and day of week, is weekend columns

-- ideas
-- credit card entries parsing logic. to payee_cleaned, item, category, tags, store_type, purchase_channel, essentiality, recurrence
-- auto-merge the credit card sheet
    -- think about whether to log credit card costs in this case. would have to limit usage to only certain cases etc. otherwise you will probably lose granularity
    -- merge and attach to cleaning pipeline: merge and normalize dates (done), populate other columns (need to build)
    -- automate merge process

with 

src as (
    select * 
    from {{ ref('src_raw_spending') }}
), 

derived_columns as (
    select 
        timestamp_datetime,
        date(timestamp_datetime) as transaction_date,
        format_datetime('%m', timestamp_datetime) as transaction_month,
        format_datetime('%a', timestamp_datetime) as transaction_day_of_week,
        case 
            when regexp_contains(format_date('%a', timestamp_datetime), r'Sat|Sun') then 1
            else 0
        end as is_weekend,
        amount,
        case when transaction_type = '💰income💰' then amount else 0 end as income,
        case when transaction_type = 'expense' then -amount else 0 end as expense,
        payment_method,
        payee,
        case 
            when regexp_contains(lower(payee), r'[Ll][Ii][Nn][Ee]\s*[Mm][Aa][Nn]') then 'Line Man'
            when regexp_contains(lower(payee), r'chat\s*gpt') then 'ChatGPT'
            when regexp_contains(lower(payee), r'ｳｴﾙﾊﾟ-ｸ') then 'Welpark'
            else payee
        end as payee_cleaned,
        item,
        category,
        case 
            when lower(trim(category)) = 'rent/utilities' then 'Housing & Utilities' 
            else category
        end as category_standardized,
        tags,
        food_details,
        hobby_details,
        trip_details,
        social,
        store_type,
        case 
            when lower(trim(store_type)) = 'restaurant' then 'Restaurant / Food Stall'
            else store_type
        end as store_type_standardized,
        purchase_channel,
        essentiality,
        recurrence_type,
        value_rating,
        notes
    from src
)

select * from derived_columns
