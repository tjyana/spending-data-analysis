    -- clean date
    -- convert the date column to a date type

with source as  (

    select 
        date,
        payee,
        amount,
        tags,
        social,
        essentiality,
        value_rating,
        category,
        store_type,
        purchase_channel,
        recurrence_type,
        Random_memos,
        payment_method,
        item,
        'credit_card' as source_system
    from {{ source('raw_spending', 'credit_card_statements') }}
    
), 

renamed as (
    select 
        date as timestamp_raw,
        payee,
        amount,
        tags,
        social,
        essentiality,
        value_rating,
        category,
        store_type,
        purchase_channel,
        recurrence_type,
        Random_memos as notes,
        payment_method,
        item,
        source_system
    from source
),

normalized as (
    select
        -- to deal with 2025/10/20 format
        datetime(timestamp_raw) as timestamp_datetime,
        'expense' as transaction_type,
        amount, 
        null as payment_method,
        trim(payee) as payee,
        trim(item) as item,
        trim(category) as category, 
        trim(tags) as tags,
        -- add the logic later in stg_derived columns
        -- trim(food_details) as food_details,
        -- trim(hobby_details) as hobby_details,
        -- trim(trip_details) as trip_details,
        trim(social) as social,
        trim(store_type) as store_type,
        trim(purchase_channel) as purchase_channel,
        trim(essentiality) as essentiality, 
        trim(recurrence_type) as recurrence_type,
        value_rating as value_rating,
        trim(notes) as notes,
        null as anomaly,
        trim(source_system) as source_system
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
        -- cast(food_details as string) as food_details,
        -- cast(hobby_details as string) as hobby_details,
        -- cast(trip_details as string) as trip_details,
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


