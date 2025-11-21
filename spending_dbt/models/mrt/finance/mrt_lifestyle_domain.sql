-- Lifestyle Domain	Categories to Group	Insight
-- Living Base	        Housing, Groceries, Utilities, Transportation	Monthly baseline cost
-- Self-Care	        Health, Personal & Shopping, Hobbies	        Physical & mental well-being investment
-- Joy & Exploration	Dining, Leisure, Travel, Media	                Happiness-driven discretionary spend
-- Financial Operations	Finance & Fees, Miscellaneous	                Meta layer (money management, irregulars)

-- This makes your dashboard summaries much more meaningful — e.g., “40% of my spending goes to Joy & Exploration, but only 10% to Self-Care.”

select *
from {{ ref('stg_add_derived_columns') }}

-- group by lifestyle domain
-- group by categories to group
-- group by insight

-- group by lifestyle domain
-- group by categories to group
-- group by insight