select *
  from {{ ref('stg_thb__202512bkk') }}
  where id_key is null 