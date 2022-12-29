import argparse
import io
import json
import logging
import yaml
import random
import uuid
import time
from typing import Optional, Dict, List
from google.cloud import aiplatform as vertex_ai
from google.cloud import bigquery
from google.cloud.aiplatform import EntityType, Feature, Featurestore
from google.api_core.exceptions import NotFound

FS_INGESTION_DATASET = 'fs_ingestion'

def create_fs_if_not_exists(project: str, location: str, featurestore_id: str) -> Featurestore:
    vertex_ai.init(project=project, location=location)
    try:
        fs = vertex_ai.Featurestore(featurestore_name=featurestore_id)
    except NotFound:
        logging.info(f'feature_store {featurestore_id} not found')
        fs = Featurestore.create(
            featurestore_id=featurestore_id,
            labels={"label": "test-fs"},
            sync=True,
        )
    return fs


def create_entity_type_if_not_exists(
        feature_store: vertex_ai.Featurestore,
        entity_type_id: str,
        description: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
) -> EntityType:
    try:
        entity_type = feature_store.get_entity_type(entity_type_id=entity_type_id)
    except NotFound:
        logging.info(f'entity_type {entity_type_id} not found')
        entity_type = feature_store.create_entity_type(
            entity_type_id=entity_type_id,
            description=description,
            labels=labels
        )
    return entity_type


def create_feature_if_not_exists(
        entity_type: vertex_ai.EntityType,
        feature_id: str,
        value_type: str,
        description: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
) -> Feature:
    try:
        feature = entity_type.get_feature(feature_id=feature_id)
    except NotFound:
        logging.info(f'feature {feature_id} not found')
        feature = entity_type.create_feature(
            feature_id=feature_id,
            value_type=value_type,
            description=description,
            labels=labels
        )
    return feature


def get_bq_schema_from_table(bq_client: bigquery.Client, full_table_id: str) -> list:
    table = bq_client.get_table(full_table_id)
    f = io.StringIO("")
    bq_client.schema_to_json(table.schema, f)
    schema = json.loads(f.getvalue())
    return schema


def bq_schema_to_fs_types(bq_schema: List[Dict[str, str]]) -> Dict[str, str]:
    mapping_dict = {
        "arrays": {
            "STRING": "STRING_ARRAY",
            "INTEGER": "INT64_ARRAY",
            "FLOAT": "DOUBLE_ARRAY",
            "BOOLEAN": "BOOL_ARRAY",
        },
        "single_value": {
            "STRING": "STRING",
            "INTEGER": "INT64",
            "FLOAT": "DOUBLE",
            "BOOLEAN": "BOOL",
        }
    }
    fs_schema = {}
    for column in bq_schema:
        mode = 'arrays' if column['mode'] == 'REPEATED' else 'single_value'
        try:
            fs_schema[column['name']] = mapping_dict[mode][column['type']]
        except KeyError:
            logging.debug(f'Column {column["name"]} will not be created  as type {column["type"]} cannot be ingested '
                          f'in the feature store')
            continue
    return fs_schema


def get_fs_types_from_bq_table(bq_client: bigquery.Client, full_table_id: str):
    bq_schema = get_bq_schema_from_table(bq_client=bq_client, full_table_id=full_table_id)
    schema_fs_types = bq_schema_to_fs_types(bq_schema=bq_schema)
    return schema_fs_types


def get_column_name_from_feature_dict(feature_dict) -> str:
    if 'column_name' in feature_dict:
        column_name = feature_dict['column_name']
    else:
        column_name = feature_dict['feature_name']
    return column_name

def get_feature_source_fields(entity_type_dict: dict) -> dict:
    feature_source_fields = {}
    for feature_dict in entity_type_dict['features']: 
        column_name =  get_column_name_from_feature_dict(feature_dict=feature_dict)
        feature_name = feature_dict['feature_name']
        feature_source_fields[feature_name] = column_name
    return feature_source_fields



def check_types_before_ingestion(
     bq_source_uri: str,
     schema_fs_types: dict,
                entity_type: EntityType,
                feature_source_fields: dict):
    features = entity_type.list_features()
    for feature in features:
        column_name = feature_source_fields[feature.name]
        feature_value_type_in_bq = schema_fs_types[column_name]
        feature_value_type_in_fs = str(feature.gca_resource.value_type.name )
        try:
            assert feature_value_type_in_bq == feature_value_type_in_fs
        except AssertionError as e:
            logging.error(f'For input table {bq_source_uri}, column {column_name} \n FS feature {feature.name} detected inconsistency between ' 
                          f'column type in BQ {feature_value_type_in_bq} and relative value in the FS {feature_value_type_in_fs}')
            raise e
    logging.info(f'Types checked successfully for entity_type {entity_type.name} in {entity_type.featurestore_name}')


def setup_fs_from_config(fs_config: dict):
    project = fs_config['experimental_feature_store']['project']
    location = fs_config['experimental_feature_store']['location']
    featurestore_id = fs_config['experimental_feature_store']['id']
    ingestion_mode = fs_config['experimental_feature_store']['ingestion_mode']
    bq_client = bigquery.Client(project=project, location=location)
    feature_store = create_fs_if_not_exists(
        project=project,
        location=location,
        featurestore_id=featurestore_id
    )
    for entity_type_dict in fs_config['feature_mapping']:
        output_full_table_id = str(bq_client.get_table(entity_type_dict['output_full_table_id']).full_table_id).replace(':','.')
        schema_fs_types = get_fs_types_from_bq_table(
            bq_client=bq_client,
            full_table_id=output_full_table_id
        )
        entity_type = create_entity_type_if_not_exists(
            feature_store=feature_store,
            entity_type_id=entity_type_dict['entity_type_id'],
            labels=entity_type_dict.get('labels'),
            description=entity_type_dict.get('description')
        )
        for feature_dict in entity_type_dict['features']:
            column_name = get_column_name_from_feature_dict(feature_dict)
            feature_value_type = schema_fs_types[column_name]
            create_feature_if_not_exists(
                entity_type=entity_type,
                feature_id=feature_dict['feature_name'],
                value_type=feature_value_type,
                labels=feature_dict.get('labels'),
                description=feature_dict.get('description')
            )
        feature_source_fields = get_feature_source_fields(entity_type_dict=entity_type_dict)
        if ingestion_mode == 'check_types_only':
            check_types_before_ingestion(
                bq_source_uri=output_full_table_id,
                schema_fs_types=schema_fs_types,
                entity_type=entity_type,
                feature_source_fields=feature_source_fields
            )
        elif ingestion_mode == 'ingest':
            logging.info(f"Beginning ingestion process for table {output_full_table_id}")
            ingestion_job = entity_type.ingest_from_bq(
                bq_source_uri=f"bq://{output_full_table_id}",
                feature_ids=[feature_dict['feature_name'] for feature_dict in entity_type_dict['features']],
                feature_time=entity_type_dict['feature_time'],
                feature_source_fields=feature_source_fields,
                entity_id_field=entity_type_dict['entity_id_field'],
                disable_online_serving=True,
                sync=True
            )
        else:
            raise ValueError(f'Value for {ingestion_mode} not supported')


def parse_args():
    parser = argparse.ArgumentParser(description="Utility tool to perform a manual ingestion in an exp feature store")
    parser.add_argument("--config_path", help="Config file for the ingestion", required=True)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    args = parse_args()
    fs_config = yaml.safe_load(open(args.config_path))
    setup_fs_from_config(fs_config=fs_config)
