"""
This file has been generated from dag_runner.j2
"""
from airflow import DAG
from openmetadata_managed_apis.workflows import workflow_factory

workflow = workflow_factory.WorkflowFactory.create("/opt/airflow/dag_generated_configs/2d744e55-b2ef-4ee9-a761-3b24a62c8f10.json")
workflow.generate_dag(globals())
dag = workflow.get_dag()