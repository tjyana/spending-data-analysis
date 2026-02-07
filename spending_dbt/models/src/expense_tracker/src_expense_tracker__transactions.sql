    -- clean date
    -- convert the date column to a date type

with source as  (
    select
        null as id_key,
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
        Random_memos,
        anomaly,
        'expense_tracker' as source_system
    from {{ source('raw_spending', 'expense_tracker') }}

),

renamed as (
    select
        id_key,
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
        Random_memos as notes,
        anomaly,
        source_system
    from source
),

normalized as (
    select
        -- to deal with 2025/10/20 format
        cast(id_key as string) as id_key,
        case
            -- YYYY/MM/DD (year-first)
            when regexp_contains(timestamp_raw, r'^\d{4}/') then parse_datetime('%Y/%m/%d %H:%M:%S', concat(timestamp_raw, ' 00:00:00'))
            -- M/D/YYYY or MM/DD/YYYY (month-first, no time)
            when regexp_contains(timestamp_raw, r'^\d{1,2}/\d{1,2}/\d{4}$') then parse_datetime('%m/%d/%Y %H:%M:%S', concat(timestamp_raw, ' 00:00:00'))
            -- Has time portion (MM/DD/YYYY HH:MM:SS)
            when regexp_contains(timestamp_raw, r'\d{1,2}:\d{2}:\d{2}') then parse_datetime('%m/%d/%Y %H:%M:%S', timestamp_raw)
            else NULL
        end as timestamp_datetime,
        transaction_type,
        amount,
        trim(payment_method) as payment_method,
        trim(payee) as payee,
        trim(item) as item,
        trim(category) as category,
        trim(tags) as tags,
        trim(food_details) as food_details,
        trim(hobby_details) as hobby_details,
        trim(trip_details) as trip_details,
        trim(social) as social,
        trim(store_type) as store_type,
        trim(purchase_channel) as purchase_channel,
        trim(essentiality) as essentiality,
        trim(recurrence_type) as recurrence_type,
        value_rating as value_rating,
        trim(notes) as notes,
        trim(anomaly) as anomaly,
        source_system
    from renamed
),

type_cast as (
    select
        id_key,
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
        cast(notes as string) as notes,
        cast(anomaly as string) as anomaly,
        cast(source_system as string) as source_system
    from normalized
)

select *
    from type_cast
