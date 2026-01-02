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
    from {{ ref('src_credit_card__statements') }}
), 

derived_columns as (
    select 
        id_key,
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
            when transaction_type = 'expense' or transaction_type is null then -amount 
            else 0 
        end as expense,
        payment_method,
        payee,
        -- subs. maybe make a diff CTE for this later
        case 
            when regexp_contains(lower(payee), r'[Ll][Ii][Nn][Ee]\s*[Mm][Aa][Nn]|lpth\*pf') then 'Line Man'
            when regexp_contains(payee, r'OPENAI') then 'OpenAI'
            when regexp_contains(lower(payee), r'ｳｴﾙﾊﾟ-ｸ') then 'Welpark'
            when regexp_contains(lower(payee), r'セブン|ｾﾌﾞﾝ') then '7-11'
            when regexp_contains(lower(payee), r'ローソン|ﾛｰｿﾝ') then 'Lawson'
            when regexp_contains(lower(payee), r'ファミリーマート') then 'Family Mart'
            when regexp_contains(lower(payee), r'東京ガス') then 'Tokyo Gas'
            when regexp_contains(lower(payee), r'ﾊﾟｽﾓ') then 'PASMO'
            when regexp_contains(lower(payee), r'ﾌﾟﾗｲﾑｶｲﾋ') then 'Amazon Prime'
            when regexp_contains(lower(payee), r'ソフトバンク') then 'Softbank'
            when regexp_contains(lower(payee), r'suno') then 'Suno'
            when regexp_contains(lower(payee), r'ココカラファインアプリ') then 'Cocokara Fine'
            when regexp_contains(lower(payee), r'ｼﾞﾔﾊﾟﾝﾋﾞﾊﾞﾚﾂｼﾞ') then 'Vending Machine'
            when regexp_contains(lower(payee), r'ﾌﾘﾎ-ﾚｽ') then 'Frijoles'
            when regexp_contains(lower(payee), r'スマートフィット') then 'Smart Fit'
            when regexp_contains(lower(payee), r'apple') then 'Apple'
            when regexp_contains(lower(payee), r'booking') then 'Booking.com'
            when regexp_contains(lower(payee), r'nok air|nokair') then 'Nok Air'
            when regexp_contains(lower(payee), r'サミット|summit') then 'Summit'
            when regexp_contains(lower(payee), r'楽天モバイル') then 'Rakuten Mobile'
            when regexp_contains(lower(payee), r'ｽﾀ-ﾊﾞﾂｸｽ') then 'Starbucks'
            when regexp_contains(lower(payee), r'ユニクロ') then 'Uniqlo'
            when regexp_contains(lower(payee), r'AMAZON\.CO\.JP') then 'Amazon'            
            else trim(regexp_replace(regexp_replace(payee, r'(ＶＩＳＡ海外利用|ＶＩＳＡ国内利用|楽天ＳＰ)', ''), r'\s+', ' '))
        end as payee_standardized,
        item,
        category,
        case 
            when lower(trim(category)) = 'rent/utilities' then 'Housing & Utilities' 
            else category
        end as category_standardized,
        tags,
        -- add finance_details later. with expense stg too
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
        anomaly,
        source_system
    from src
),

fill_ins as (
    select 
        id_key,
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
        case 
            when item is null and regexp_contains(payee_standardized, r'Line Man') then 'Delivery Order'
            else item
        end as item_fillin,
        category,
        category_standardized,
        case
                -- case when for credit card statement
                -- keep out list: Amazon, Starbucks, Uniqlo
            when category is null and regexp_contains(payee_standardized, r'Line Man') then 'Miscellaneous & Gifts'
            when category is null and regexp_contains(payee_standardized, r'OpenAI|Amazon Prime|Suno|Apple') then 'Media & Subscriptions'
            when category is null and regexp_contains(payee_standardized, r'Welpark|Cocokara Fine') then 'Household Supplies'
            when category is null and regexp_contains(payee_standardized, r'7-11|Lawson|Family Mart|Vending Machine|Frijoles') then 'Dining & Cafes'
            when category is null and regexp_contains(payee_standardized, r'Tokyo Gas|Softbank|Rakuten Mobile') then 'Housing & Utilities'
            when category is null and regexp_contains(payee_standardized, r'PASMO') then 'Transportation'
            when category is null and regexp_contains(payee_standardized, r'Smart Fit') then 'Health & Wellness'
            when category is null and regexp_contains(payee_standardized, r'Summit') then 'Groceries'
            else category_standardized
        end as category_complete,
        tags,
        case 
            when tags is null and regexp_contains(payee_standardized, r'7-11|Lawson|Family Mart|Vending Machine') then 'food: snack'
            else tags
        end as tags_complete,
        social,
        store_type,
        store_type_standardized,
                -- store_type_complete
                -- for credit card statements
                -- for before november

        purchase_channel,
            -- for credit card statements
            -- for before november
            -- just do online for whatever, and then else in-store
        essentiality,
            -- this might be tough. decide later
        recurrence_type,
            -- recurrence_type_complete
            -- for credit card statements
            -- for before november
            -- should be easy. based off category?
        value_rating,
            -- changed in november. might be tough
        notes,
        anomaly,
        source_system
    from derived_columns
),

final as (
    select 
        id_key,
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
        payment_method_complete,
        payee,
        payee_standardized,
        item,
        category,
        category_standardized,
        category_complete,
        tags,
        tags_complete,
        case 
            when regexp_contains(tags_complete, r'^food: ')
                then regexp_replace(tags_complete, r'^[^:]+:\s*', '') 
                else null 
            end as food_details,
        case 
            when regexp_contains(tags_complete, r'^hobby: ')
                then regexp_replace(tags_complete, r'^[^:]+:\s*', '') 
                else null
            end as hobby_details,
        case    
            when regexp_contains(tags_complete, r'^trip: ')
                then regexp_replace(tags_complete, r'^[^:]+:\s*', '') 
                else null 
            end as trip_details,
        social,
        social as social_complete,
        store_type,
        store_type_standardized,
        case 
            when regexp_contains(payee_standardized, r'Line Man|OpenAI|Tokyo Gas|PASMO|Amazon|Softbank|Suno|Smart Fit|Apple|Booking|Nok|Rakuten Mobile') then 'Everything else'
            when regexp_contains(payee_standardized, r'Welpark') then 'Drugstore'
            when regexp_contains(payee_standardized, r'7-11|Lawson|Family Mart') then 'Convenience Store'
            when regexp_contains(payee_standardized, r'Summit') then 'Grocery Store'
            when regexp_contains(payee_standardized, r'Frijoles') then 'Restaurant / Food Stall' 
            else null 
        end as store_type_complete,
        purchase_channel,
        case 
            when regexp_contains(lower(payee_standardized), r'line man|openai|tokyo gas|pasmo|amazon|softbank|suno|smart fit|apple|booking|nok|rakuten mobile') then 'Online'
            when regexp_contains(payee_standardized, r'Welpark|7-11|Lawson|Family Mart|Cocokara Fine|Vending Machine|Frijoles|Summit') then 'In-Store'
            else purchase_channel
        end as purchase_channel_complete,
        essentiality,
        -- can't take into account conbini so will have to mark
        case 
            when regexp_contains(payee_standardized, r'Suno|Frijoles|Vending Machine') then 'Want'
            when regexp_contains(payee_standardized, r'Line Man|OpenAI|Amazon Prime|Apple|Booking|Nok|Smart Fit') then 'Nice-to-Have (Comfort Base)'
            when regexp_contains(payee_standardized, r'Tokyo Gas|PASMO|Softbank|Summit|Rakuten Mobile') then 'Need (Base)'
            else null 
        end as essentiality_complete,
        recurrence_type,
        case 
            when regexp_contains(payee_standardized, r'OpenAI|Tokyo Gas|Amazon|Softbank|Suno|Smart Fit|Apple|Rakuten Mobile') then 'Subscription/Automatic'
            when regexp_contains(payee_standardized, r'Line Man|Welpark|7-11|Lawson|Family Mart|PASMO|Cocokara Fine|Vending Machine|Summit|Frijoles') then 'Variable / Occasional'
            when regexp_contains(lower(payee_standardized), r'booking|nok') then 'One-Off'
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
        anomaly,
        source_system
    from fill_ins
)


select * from final
