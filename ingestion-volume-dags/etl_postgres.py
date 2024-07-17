from airflow.configuration import conf
from airflow.decorators import dag, task
from datetime import timedelta, datetime
from airflow.models.baseoperator import chain
from airflow.models import Variable

conf.set('core', 'enable_xcom_pickling', 'True')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 30, 9, 0, 0),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

blob_storage_raw_files_conn_string = Variable.get("blob_storage_raw_files_conn_string")
blob_storage_raw_files_conn_string_container_name = Variable.get("blob_storage_raw_files_conn_string_container_name")
postgres_etl_db_conn_str = Variable.get("postgres_etl_db_conn_str")

@dag(default_args=default_args, schedule_interval=None, description='ETL', catchup=False)
def etl():
    """Airflow DAG for the PostgreSQL ETL process."""

    @task
    def extract_data(path, file_type, columns=None, delimiter=','):
        from src.extractor import Extractor
        from src.reader import BlobStorageReader

        conn_str = blob_storage_raw_files_conn_string
        container_name = blob_storage_raw_files_conn_string_container_name
        blob_reader = BlobStorageReader(conn_str, container_name)

        extractor = Extractor(blob_reader, file_type, path, delimiter=delimiter, columns=columns)
        data = extractor.extract()
        return data
    
    @task
    def transform_data(data, transformation_type):
        from src.transformer import Transformer
        transformer = Transformer(data, transformation_type)
        transformer.transform()
        transformed_data = transformer.get_transformed_df()
        return transformed_data

    @task
    def load_staging(data, target_table):
        if data is None:
            print("No data to load.")
            return
        
        print("DataFrame information before loading:")
        from src.loader import Loader
        from src.connector import Connector

        connector_string = postgres_etl_db_conn_str
        print(f"Connecting with: {connector_string}")
        connector = Connector(connector_string)

        loader = Loader(connector=connector)
        try:
            print(f"Checking existence of the table {target_table}...")
            loader.check_table_exists(target_table)  
            print(f"Attempting to load data into the table {target_table}...")
            loader.load_to_db(data, 'staging', target_table)
            print("Data successfully loaded.")
        except Exception as e:
            print(f"Failed to load data: {e}")
        return 'ok'

    @task
    def load_transformed(sql_file_path):
        if sql_file_path is None:
            print("No data to load.")
            return
        
        print("DataFrame information before loading:")
        from src.loader import Loader
        from src.connector import Connector

        connector_string = postgres_etl_db_conn_str
        print(f"Connecting with: {connector_string}")
        connector = Connector(connector_string)

        loader = Loader(connector=connector)
        try:
            loader.run_query_from_file(sql_file_path)
        except Exception as e:
            print(f"Failed to execute sql: {e}")
        return 'ok'
    
    @task
    def load_dw(sql_file_path):
        if sql_file_path is None:
            print("No data to load.")
            return
        
        print("DataFrame information before loading:")
        from src.loader import Loader
        from src.connector import Connector

        connector_string = postgres_etl_db_conn_str
        connector = Connector(connector_string)

        loader = Loader(connector=connector)
        try:
            loader.run_query_from_file(sql_file_path)
        except Exception as e:
            print(f"Failed to execute sql: {e}")
        return 'ok'

    # Extract tasks
    leads_data = extract_data.override(task_id='extract_leads')(path='customer_leads_funnel.csv', file_type='csv', columns=['device_id', 'lead_id', 'registered_at', 'credit_decision', 'credit_decision_at', 'signed_at', 'revenue'])
    pageview_data = extract_data.override(task_id='extract_pageview')(path='pageview.txt', file_type='csv', columns=['ips', 'device_id', 'refer'], delimiter="|")
    fb_data = extract_data.override(task_id='extract_facebook')(path='facebook_ads_media_costs.jsonl', file_type='json')
    google_data = extract_data.override(task_id='extract_google')(path='google_ads_media_costs.jsonl', file_type='json')

    # Transform tasks
    transformed_leads = transform_data.override(task_id='transform_leads')(leads_data, 'leads')
    transformed_pageview = transform_data.override(task_id='transform_pageview')(pageview_data, 'pageview')
    transformed_fb = transform_data.override(task_id='transform_facebook')(fb_data, 'facebook')
    transformed_google = transform_data.override(task_id='transform_google')(google_data, 'google')

    # Load staging tasks
    load_staging_leads = load_staging.override(task_id='load_staging_leads')(transformed_leads, 'stg_leads')
    load_staging_pageview = load_staging.override(task_id='load_staging_pageview')(transformed_pageview, 'stg_pageview')
    load_staging_fb = load_staging.override(task_id='load_staging_facebook')(transformed_fb, 'stg_facebook')
    load_staging_google = load_staging.override(task_id='load_staging_google')(transformed_google, 'stg_google')

    # Load transformed tasks
    load_transformed_aggregated_campaigns = load_transformed.override(task_id='load_transformed_campaigns')(sql_file_path='dags/sql/insert_transformed_aggregated_campaigns.sql')
    load_transformed_pageview = load_transformed.override(task_id='load_transformed_pageview')(sql_file_path='dags/sql/insert_transformed_pv.sql')
    load_transformed_leads = load_transformed.override(task_id='load_transformed_leads')(sql_file_path='dags/sql/insert_transformed_leads.sql')

    # Load DW tasks
    load_dw_dim_ad_creative = load_dw.override(task_id='load_dw_dim_ad_creative')(sql_file_path='dags/sql/insert_dw_dim_ad_creative.sql')
    load_dw_dim_date = load_dw.override(task_id='load_dw_dim_date')(sql_file_path='dags/sql/insert_dw_dim_date.sql')
    load_dw_fact_campaign_performance = load_dw.override(task_id='load_dw_fact_campaign_performance')(sql_file_path='dags/sql/insert_dw_fact_campaign_performance.sql')
    load_dw_fact_leads = load_dw.override(task_id='load_dw_fact_leads')(sql_file_path='dags/sql/insert_dw_fact_leads.sql')
    load_dw_dim_campaign = load_dw.override(task_id='load_dw_dim_campaign')(sql_file_path='dags/sql/insert_dim_campaign.sql')


    load_staging_pageview >> load_transformed_pageview >> load_transformed_leads
    load_staging_leads >> load_transformed_leads
    load_staging_google >> load_transformed_aggregated_campaigns 
    load_staging_fb >> load_transformed_aggregated_campaigns 

    load_transformed_leads >> load_dw_dim_date >> load_dw_fact_leads

    load_transformed_aggregated_campaigns >> load_dw_dim_campaign
    load_transformed_aggregated_campaigns >> load_dw_dim_ad_creative
    load_transformed_aggregated_campaigns >> load_dw_dim_date

    load_dw_dim_ad_creative, load_dw_dim_date >> load_dw_fact_campaign_performance

etl_dag = etl()
