with 

src as (
    select * 
    from {{ source('raw_spending', 'thb_202512bkk') }}
), 

renamed as (
    select 
        id_key,
        Timestamp as timestamp_raw,
        'expense' as transaction_type,
        amount, 
        payment_method,
        payee,
        item,
        category, 
        tags,
        IFNULL(TRIM(REGEXP_EXTRACT(tags, r'food:\s*([^,;|]+)')), '') as food_details,
        IFNULL(TRIM(REGEXP_EXTRACT(tags, r'hobbies:\s*([^,;|]+)')), '') as hobby_details,
        '2025/12 Bangkok' as trip_details,
        social,
        store_type,
        purchase_channel,
        essentiality, 
        recurrence_type,
        value_rating,
        Random_memos as notes,
        anomaly_purchase as anomaly,
        'thb' as source_system
    from src
),

normalized as (
    select
        -- to deal with 2025/10/20 format
        timestamp_raw,
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
        anomaly,
        source_system
    from renamed
),

type_cast as (
    select 
        timestamp_raw,
        cast(transaction_type as string) as transaction_type,
        cast(amount as numeric) as amount, 
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