with 

src as (
    select * 
    from {{ source('raw_spending', '202512_BKK') }}
), 

renamed as (
    select 
        Timestamp as timestamp_raw,
        'expense' as transaction_type,
        amount, 
        payment_method,
        payee,
        item,
        category, 
        tags,
        null as food_details,
        null as hobby_details,
        null as trip_details,
        social,
        store_type,
        purchase_channel,
        essentiality, 
        recurrence_type,
        value_rating,
        Random_memos as notes,
        Non_typical_big_purchase____Subtract_from_typical_monthly__ as anomaly,
        'thb' as source_system
    from src
)

select * from renamed