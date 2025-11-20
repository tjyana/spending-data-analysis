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
        format_datetime('%m/%Y', timestamp_datetime) as transaction_month_year,
        case 
            when regexp_contains(format_date('%a', timestamp_datetime), r'Sat|Sun') then 1
            else 0
        end as is_weekend,
        amount,
        transaction_type,
        case when transaction_type = '💰income💰' then amount else 0 end as income,
        case when transaction_type = 'expense' then -amount else 0 end as expense,
        payment_method,
        payee,
        -- subs. maybe make a diff CTE for this later
        case 
            when regexp_contains(lower(payee), r'[Ll][Ii][Nn][Ee]\s*[Mm][Aa][Nn]') then 'Line Man'
            when regexp_contains(lower(payee), r'OPENAI') then 'OpenAI'
            when regexp_contains(lower(payee), r'ｳｴﾙﾊﾟ-ｸ') then 'Welpark'
            when regexp_contains(lower(payee), r'セブン|ｾﾌﾞﾝ') then '7-11'
            when regexp_contains(lower(payee), r'ローソン|ﾛｰｿﾝ') then 'Lawson'
            when regexp_contains(lower(payee), r'ファミリーマート') then 'Family Mart'
            when regexp_contains(lower(payee), r'東京ガス') then 'Tokyo Gas'
            when regexp_contains(lower(payee), r'ﾊﾟｽﾓ') then 'PASMO'
            when regexp_contains(lower(payee), r'ﾌﾟﾗｲﾑｶｲﾋ') then 'Amazon Prime'
            when regexp_contains(lower(payee), r'ソフトバンク') then 'Softbank'
            when regexp_contains(lower(payee), r'SUNO INC') then 'Suno'
            when regexp_contains(lower(payee), r'ココカラファインアプリ') then 'Cocokara Fine'
            when regexp_contains(lower(payee), r'ｼﾞﾔﾊﾟﾝﾋﾞﾊﾞﾚﾂｼﾞ') then 'Vending Machine'
            when regexp_contains(lower(payee), r'ﾌﾘﾎ-ﾚｽ') then 'Frijoles'
            when regexp_contains(lower(payee), r'スマートフィット') then 'Smart Fit'
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
),

fill_ins as (
    select 
        timestamp_datetime,
        transaction_date,
        transaction_month,
        transaction_month_year,
        transaction_day_of_week,
        is_weekend,
        amount,
        transaction_type,
        income,
        expense,
        payment_method,
        case
            -- for credit card statements
            when regexp_contains(payee, r'ＶＩＳＡ|ソフトバンク|スマートフィット１００|LINEPAY|東京ガス|利用国US|楽天モバイル通信料') then 'Credit Card'
            when regexp_contains(payee, r'ＪＣＢ|ｼﾞﾔﾊﾟﾝﾋﾞﾊﾞﾚﾂｼﾞﾎ-|ｳｴﾙﾊﾟ-ｸ') then 'QuicPay'
            when regexp_contains(payee, r'ﾓﾊﾞｲﾙﾊﾟｽﾓﾁﾔ-ｼﾞ') then 'Apple Pay'
            when regexp_contains(payee, r'楽天ＳＰ') then 'Rakuten Pay'
            else payment_method
        end as payment_method_standardized,
        payee,
        payee_cleaned,
        item,
        category,
        category_standardized,
        tags,
        food_details,
        hobby_details,
        trip_details,
        social,
        store_type,
        store_type_standardized,
        purchase_channel,
        essentiality,
        recurrence_type,
        value_rating,
        notes
    from derived_columns
)

select * from fill_ins
