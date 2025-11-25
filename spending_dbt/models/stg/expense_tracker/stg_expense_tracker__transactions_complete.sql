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
    from {{ ref('src_expense_tracker__transactions') }}
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
        case 
            when transaction_type = '💰income💰' then amount 
            else 0 
        end as income,
        case 
            when transaction_type = 'expense' then -amount 
            else 0 
        end as expense,
        payment_method,
        payee,
        -- subs. maybe make a diff CTE for this later
        case 
            when regexp_contains(lower(payee), r'[Ll][Ii][Nn][Ee]\s*[Mm][Aa][Nn]') then 'Line Man'
            when regexp_contains(lower(payee), r'openai') then 'OpenAI'
            when regexp_contains(lower(payee), r'ｳｴﾙﾊﾟ-ｸ') then 'Welpark'
            when regexp_contains(lower(payee), r'セブン|ｾﾌﾞﾝ') then '7-11'
            when regexp_contains(lower(payee), r'ローソン|ﾛｰｿﾝ') then 'Lawson'
            when regexp_contains(lower(payee), r'ファミリーマート') then 'Family Mart'
            when regexp_contains(lower(payee), r'東京ガス') then 'Tokyo Gas'
            when regexp_contains(lower(payee), r'ﾊﾟｽﾓ') then 'PASMO'
            when regexp_contains(lower(payee), r'ﾌﾟﾗｲﾑｶｲﾋ') then 'Amazon Prime'
            when regexp_contains(lower(payee), r'amazon') then 'Amazon'
            when regexp_contains(lower(payee), r'ソフトバンク') then 'Softbank'
            when regexp_contains(lower(payee), r'suno') then 'Suno'
            when regexp_contains(lower(payee), r'ココカラファインアプリ') then 'Cocokara Fine'
            when regexp_contains(lower(payee), r'ｼﾞﾔﾊﾟﾝﾋﾞﾊﾞﾚﾂｼﾞ') then 'Vending Machine'
            when regexp_contains(lower(payee), r'ﾌﾘﾎ-ﾚｽ') then 'Frijoles'
            when regexp_contains(lower(payee), r'スマートフィット') then 'Smart Fit'
            when regexp_contains(lower(payee), r'apple') then 'Apple'
            when regexp_contains(lower(payee), r'booking') then 'Booking.com'
            when regexp_contains(lower(payee), r'nok') then 'Nok Air'
            when regexp_contains(lower(payee), r'サミット|summit') then 'Summit'
            when regexp_contains(lower(payee), r'family mart') then 'Family Mart'
            when regexp_contains(lower(payee), r'new days') then 'New Days'
            when regexp_contains(lower(payee), r'banh\s*mi\s*xin\s*chao') then 'Banh Mi Xin Chao'
            when regexp_contains(lower(payee), r'楽天モバイル通信料|mobile') then 'Rakuten Mobile'
            else payee
        end as payee_standardized,
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
        notes,
        source_system
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
        end as payment_method_complete,
        payee,
        payee_standardized,
        item,
        category,
        category_standardized,
        case
                -- case when for credit card statement
            when regexp_contains(payee_standardized, r'Line Man') then 'Miscellaneous & Gifts'
            when regexp_contains(payee_standardized, r'OpenAI|Amazon Prime|Suno|Apple') then 'Media & Subscriptions'
            when regexp_contains(payee_standardized, r'Welpark|Cocokara Fine') then 'Household Supplies'
            when regexp_contains(payee_standardized, r'7-11|Lawson|Family Mart|Vending Machine|Frijoles|Sakaeya|New Days|Starbucks|Kuminoya|Hoshino|Banh Mi') then 'Dining & Cafes'
            when regexp_contains(food_details, r'breakfast|lunch|dinner|snack') then 'Dining & Cafes'
            when regexp_contains(payee_standardized, r'Tokyo Gas|Softbank') then 'Housing & Utilities'
            when regexp_contains(payee_standardized, r'PASMO') then 'Transportation'
            when regexp_contains(payee_standardized, r'Smart Fit') then 'Health & Wellness'
            when regexp_contains(payee_standardized, r'Summit') then 'Groceries'
            when regexp_contains(payee_standardized, r'Booking.com') then 'Travel & Experiences'
            when regexp_contains(payee_standardized, r'Uniqlo') then 'Personal & Shopping'

            else category_standardized
        end as category_complete,
                -- category_complete
                -- case when for credit card statements
                -- case when for october
        tags,
        case 
            when regexp_contains(payee_standardized, r'7-11|Lawson|Family Mart|Vending Machine') then 'food: snack'
            else tags
        end as tags_complete,
                -- tags_complete
                -- case when for credit card statements
        food_details,
        hobby_details,
        trip_details,
        social,
        social as social_complete,
        store_type,
        store_type_standardized,
        store_type_standardized as store_type_complete,
                -- store_type_complete
                -- for credit card statements
                -- for before november

        purchase_channel,
        purchase_channel as purchase_channel_complete,
            -- for credit card statements
            -- for before november
            -- just do online for whatever, and then else in-store
        essentiality,
        essentiality as essentiality_complete,
            -- this might be tough. decide later
        recurrence_type,
        case 
            when regexp_contains(payee_standardized, r'^OpenAI|Tokyo Gas|Amazon|Softbank|Suno|Smart Fit|Apple') then 'Subscription/Automatic'
            when regexp_contains(payee_standardized, r'^Line Man|Welpark|7-11|Lawson|Family Mart|PASMO|Cocokara Fine|Vending Machine|Summit|Frijoles|Summit|') then 'Variable / Occasional'
            when regexp_contains(payee_standardized, r'^Booking|Nok') then 'One-Off'
            when regexp_contains(payee_standardized, r'^Water Bill|Rent') then 'Recurring'
            else null 
        end as recurrence_type_complete,
            -- recurrence_type_complete
            -- for credit card statements
            -- for before november
            -- should be easy. based off category?
        value_rating,
        value_rating as value_rating_complete,
            -- changed in november. might be tough
        notes,
        source_system
    from derived_columns
)

select * from fill_ins
