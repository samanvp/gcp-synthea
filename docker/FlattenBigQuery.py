#python3 -m venv venv3
#. venv3/bin/activate
#python3 -m pip install requests
#python3 -m pip install google-api-core
#python3 -m pip install google-cloud-bigquery

import json
import math
import pathlib
import re
import sys
from concurrent import futures
from google.api_core import exceptions as api_exceptions
from google.cloud import bigquery
from google.cloud import exceptions

JOIN_TEMPLATE = " LEFT JOIN\n    {0} AS {1}"
SELECT_TEMPLATE = "{} AS {}"
FLATTEN_TABLE_QUERY = (
    'SELECT {SELECTS}\nFROM `{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}` AS t'
    '{JOINS}')
FLATTEN_SUFFIX = '_flatten'

class FlattenBigQuery(object):
  def __init__(self):
    self.bq_client = bigquery.Client()
    self.project_id = self.bq_client.project
    self.table_drop = None
    self.field_drop = None
    self.bq_ds_drop = None
    self.formatter_was_run = False

  def _flatten_dataset(self, bq_dataset):
    """Flattens all tables in the given dataset."""
    tables = self.bq_client.list_tables(bq_dataset)  # Make an API request.

    for table in tables:
      print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
      self._flatten_table(table.dataset_id, table.table_id,
                          table.dataset_id, table.table_id + FLATTEN_SUFFIX, False)
      print("==================================")

  def _flatten_table(self,
                     source_bq_dataset,
                     source_table_id,
                     output_bq_dataset,
                     output_table_id,
                     verbose=False):
    """Flatten table and output into a view"""
    print("Extract schema...")
    schema = self._extract_schema(source_bq_dataset, source_table_id)
    print("Build flattening query...")
    selects, joins = self._expand_fields("t", schema)
    query_sql = FLATTEN_TABLE_QUERY.format(
        SELECTS=",\n    ".join(selects),
        PROJECT_ID=self.project_id,
        BQ_DATASET=source_bq_dataset,
        TABLE_NAME=source_table_id,
        JOINS="".join(joins))

    create_view = "CREATE VIEW " + output_bq_dataset + "." + output_table_id + " AS \n" + query_sql
    if verbose:
      print("Query to be run:\n{}\n".format(create_view))

    print("Create view...")
    view_id = "{}.{}.{}".format(self.project_id, output_bq_dataset,
                                output_table_id)
    view = bigquery.Table(view_id)
    view.view_query = query_sql
    # Make an API request to create the view.
    try:
      view = self.bq_client.create_table(view)
      print("Query results loaded to the table {}".format(view_id))
    except api_exceptions.Conflict as e:
      print("Failed to create view for table {}. Exception received: {}".format(
          view_id, e))
  def _extract_schema(self, bq_dataset, table_id):
    dataset_ref = bigquery.DatasetReference(self.project_id, bq_dataset)
    table_ref = dataset_ref.table(table_id)
    table = self.bq_client.get_table(table_ref)
    return table.schema
  def _expand_fields(self, parent, fields):
    """Recursively extract select and join clauses for the flattening query."""
    selects = []
    joins = []
    for field in fields:
      field_path = "{}.{}".format(parent, field.name)
      field_alias = field_path.replace(".", "_")
      field_alias = field_alias[2:] if field_alias[:2] == "t_" else field_alias
      if field.field_type == "RECORD":
        if field.mode == "REPEATED":
          joins.append(JOIN_TEMPLATE.format(field_path, field_alias))
          sub_selects, sub_joins = self._expand_fields(field_alias,
                                                       field.fields)
        else:
          sub_selects, sub_joins = self._expand_fields(field_path, field.fields)
        selects.extend(sub_selects)
        joins.extend(sub_joins)
      else:
        if field.mode == "REPEATED":
          joins.append(JOIN_TEMPLATE.format(field_path, field_alias))
          selects.append(field_alias)
        else:
          selects.append(SELECT_TEMPLATE.format(field_path, field_alias))
    return selects, joins
  def _get_empty_tables(self, bq_dataset):
    """Return empty tables in the dataset."""
    list_of_empty_tables = []
    for table_name in self._get_table_list(bq_dataset):
      raw_query = """
        SELECT COUNT(0) = 0 AS is_empty
        FROM `{0}.{1}.{2}`
        """.format(self.project_id, bq_dataset, table_name)
      is_empty_result = self._run_query(raw_query)
      if is_empty_result.iloc[0]["is_empty"]:
        list_of_empty_tables.append(table_name)
    return list_of_empty_tables


def main():
    flatter = FlattenBigQuery()

    if len(sys.argv) < 2:
      print('You need to set dataset_id as input args')
      exit(1)
    elif len(sys.argv) == 2:
      flatter._flatten_dataset(sys.argv[1])
    elif len(sys.argv) == 3:
      flatter._flatten_table(sys.argv[1], sys.argv[2], sys.argv[1], sys.argv[2] + FLATTEN_SUFFIX, False)
    else:
      print('You can have at most 2 input args:  dataset_id and table_id')
      exit(1)
    print("All done!")

if __name__ == "__main__":
    main()
