tasks:
- find missing thb data (94 rows but models only showing like 79?)

Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


thb models:

src_thb__202512bkk ✅
Role: raw rename-only
direct mapping from BigQuery source
no logic
no filtering
no joins

stg_thb__202512bkk ✅
Role: standardized THB transactions
cast types
normalize column names
ensure all canonical columns exist (even if NULL)
align to the 40-column contract
❗️This should not filter cash vs card
❗️This should not join to CC

int_transactions__credit_card_enriched　✅
(current int_finance__credit_card_enriched that takes credit card entries from stg_thb and joins with cc statements)
Role: statement-driven card transactions enriched with THB context
Takes:
- CC statements (stg)
- THB rows with id_key
outputs: canonical transactions rows
THB fills in missing context
CC provides amount / dates
✅ exactly what you already built
✅ good place for join logic
❌ should not include cash rows

int_transactions__thb_cash 🚧
(takes stg_thb and filters out cash only)
Role: THB cash-only transactions
Takes:
- stg_thb__202512bkk
- filters to cash only

int_transactions__all 🚧
(current int_finance__transactions_union that union alls expense tracker, credit card enriched, and thb cash)
Role: canonical ledger (union point)
Requirements:
same column list
same order
same types
✅ this becomes the single source of truth for marts
