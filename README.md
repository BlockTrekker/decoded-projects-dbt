# Decode Projects
This code is intended to decode public blockchain data into a table for each function and evt per contract. This is to make a series of smaller tables for users to directly query based on individual contract actions.

The steps look like:
1. Collect current and previous ABIs for all contracts possible with naming lables
2. Use seeds with DBT to add them as a table in Google Big Query
3. Utilize UDFs PARSE_ABI_EVENTS and PARSE_ABI_FUNCTIONS to decode all function calls and evt calls
4. Use Python to create table builders within the BQ API each evt or function call 

It uses the Big Query public data set to combine coABIs   

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
