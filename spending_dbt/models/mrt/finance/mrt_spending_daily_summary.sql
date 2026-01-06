
with 
stg as (
    select *
    from {{ ref('int_transactions__all') }}

),

aggregations as (
    select 
        timestamp_datetime,
        transaction_date,
        transaction_month,
        transaction_day_of_week,
        is_weekend,
        amount,
        income,
        expense,
        payment_method,
        payee,
        item,
        category_standardized,
        tags,
        food_details,
        hobby_details,
        trip_details,
        social,
        store_type_standardized,
        purchase_channel,
        essentiality,
        recurrence_type,
        value_rating,
        notes
    from stg
)
select * from aggregations

