-- -- int_finance__credit_card_enriched.sql

with cc as (
  select *
  from {{ ref('stg_credit_card__statements_complete') }}
),

thb as (
  select *
  from {{ ref('stg_thb__202512bkk') }}  -- or whatever your THB stg ref is
),

enriched as (
  select
    -- keep statement fields as the base
    cc.*,

    -- add THB enrichment fields (pick what you actually need)
    thb.transaction_type as thb_transaction_type,
    thb.category         as thb_category,
    thb.tags             as thb_tags,
    thb.food_details     as thb_food_details,
    thb.hobby_details    as thb_hobby_details,
    thb.trip_details     as thb_trip_details,
    thb.notes            as thb_notes,

    -- optional: debugging / lineage
    case
      when thb.id_key is not null then 'matched'
      else 'unmatched'
    end as thb_match_status

  from cc
  left join thb
    on cc.id_key = thb.id_key
)

select * from enriched