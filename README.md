# Showcasing Feature Engineering with Data Engineering tools

## Existing features
- [x] Feature Engineering with DBT
- [x] Feature Engineering with Dataform Cloud
- [x] Compatibility with Dataform Core
- [ ] DBT with python models


### Download data in your BQ Environmnet
```commandline
python tools/copy_bigquery_data.py --project vertex-ai-test-365213 --location  europe-west2   
```

## Performing feature engineering with DBT
### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Learn how to setup DBT in your environment [in this doc](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)

### Setup the environment
```commandline
virtualenv venv && source venv/bin/activate && pip install -r requirements.txt
```

### Create features

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

## Performing feature engineering with Dataform core

> The same code can be also executed in Dataform Cloud by copying the files in a workspace. 

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Learn how to setup DBT in your environment [in this doc](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)

### Create features
```commandline
npm i -g @dataform/cli@^2.0.0 
dataform install
dataform init-creds bigquery
```

### Using the project

To build features:
```commandline
(cd features/dataform_project && dataform run --full-refresh)
```
To run data assertions (no tests are defined at the moment):
```commandline
(cd features/dataform_project && dataform test)
```

To test the feature creation and ingestion process on a dummy feature store:
Install dependencies:
```commandline
virtualenv venv && source venv/bin/activate && pip install -r requirements.txt
```
Perform ingestion:
```commandline
python tools/manual_ingestion_to_fs.py --config_path features/dataform_project/fs_config.yml
```

