"""
This file has been generated from dag_runner.j2
"""
from airflow import DAG
from openmetadata_managed_apis.workflows import workflow_factory

workflow = workflow_factory.WorkflowFactory.create("/opt/airflow/dag_generated_configs/4659bc73-25dd-42e9-989f-3a24d723bd4e.json")
workflow.generate_dag(globals())
dag = workflow.get_dag()