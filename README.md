# Showcasing Feature Engineering with Data Engineering tools

## Existing features
- [x] Feature Engineering with DBT
- [x] Feature Engineering with Dataform Cloud
- [x] Compatibility with Dataform Core
- [ ] DBT with python models

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Learn how to setup DBT in your environment [in this doc](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)

### Setup the environment
```commandline
virtualenv venv && source venv/bin/activate && pip install -r requirements.txt
```
### Download data in your BQ Environmnet
```commandline
python tools/copy_bigquery_data.py --project vertex-ai-test-365213 --location  europe-west2   
```
### Using the project

To build features:
```commandline
(cd features/dbt_project && dbt run)
```
To run data assertions:
```commandline
(cd features/dbt_project && dbt test)
```

To test the feature creation and ingestion process on a dummy feature store:
```commandline
python tools/manual_ingestion_to_fs.py --config_path features/dbt_project/fs_config.yml
```

