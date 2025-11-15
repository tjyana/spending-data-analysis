    -- clean date
    -- convert the date column to a date type

with source as  (

    select 
        Timestamp,
        transaction_type,
        amount, 
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
        Random_memos
    from {{ source('raw_spending', 'expense_tracker') }}
    
), 

renamed as (
    select 
        Timestamp as timestamp_raw,
        transaction_type,
        amount, 
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
        Random_memos as notes
    from source
),

normalized as (
    select
        -- to deal with 2025/10/20 format
        case
            -- YYYY/MM/DD (year-first)
            when regexp_contains(timestamp_raw, r'^\d{4}/') then parse_datetime('%Y/%m/%d %H:%M:%S', concat(timestamp_raw, ' 00:00:00'))
            -- M/D/YYYY or MM/DD/YYYY (month-first, no time)
            when regexp_contains(timestamp_raw, r'^\d{1,2}/\d{1,2}/\d{4}$') then parse_datetime('%m/%d/%Y %H:%M:%S', concat(timestamp_raw, ' 00:00:00'))
            -- Has time portion (MM/DD/YYYY HH:MM:SS)
            when regexp_contains(timestamp_raw, r'\d{1,2}:\d{2}:\d{2}') then parse_datetime('%m/%d/%Y %H:%M:%S', timestamp_raw)
            else NULL
        end as timestamp_datetime,

        --     when timestamp_raw not like '% %' then
        --         concat(
        --             substr(timestamp_raw, 6, 2), 
        --             '/', 
        --             substr(timestamp_raw, 9, 2),
        --             '/', 
        --             substr(timestamp_raw, 1, 4),
        --             ' 00:00:00'
        --         )
        --     else timestamp_raw
        -- end as timestamp_normalized,
        transaction_type,
        amount, 
        trim(lower(payment_method)) as payment_method,
        trim(lower(payee)) as payee,
        trim(lower(item)) as item,
        trim(lower(category)) as category, 
        trim(lower(tags)) as tags,
        trim(lower(food_details)) as food_details,
        trim(lower(hobby_details)) as hobby_details,
        trim(lower(trip_details)) as trip_details,
        trim(lower(social)) as social,
        trim(lower(store_type)) as store_type,
        trim(lower(purchase_channel)) as purchase_channel,
        trim(lower(essentiality)) as essentiality, 
        trim(lower(recurrence_type)) as recurrence_type,
        value_rating as value_rating,
        trim(lower(notes)) as notes
    from renamed
),

type_cast as (
    select 
        timestamp_datetime,
        cast(transaction_type as string) as transaction_type,
        cast(amount as int64) as amount, 
        cast(payment_method as string) as payment_method,
        cast(payee as string) as payee,
        cast(item as string) as item,
        cast(category as string) as category, 
        cast(tags as string) as tags,
        cast(food_details as string) as food_details,
        cast(hobby_details as string) as hobby_details,
        cast(trip_details as string) as trip_details,
        cast(social as string) as social,
        cast(store_type as string) as store_type,
        cast(purchase_channel as string) as purchase_channel,
        cast(essentiality as string) as essentiality,
        cast(recurrence_type as string) as recurrence_type,
        cast(value_rating as int64) as value_rating,
        cast(notes as string) as notes
    from normalized
)

select * 
    from type_cast


