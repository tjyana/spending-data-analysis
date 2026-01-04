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

src_thb__202512bkk 
stg_thb__202512bkk 
int_transactions__credit_card_enriched (current int_finance__credit_card_enriched that takes credit card entries from stg_thb and joins with cc statements) 
int_transactions__thb_cash (takes stg_thb and filters out cash only) 
int_transactions__all (current int_finance__transactions_union that union alls expense tracker, credit card enriched, and thb cash)