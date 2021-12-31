from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

"""
files land in the gcs bucket |--> list object in the bucket |--> load data to bigquery |--> create new_table with the latest car status |--> move list object to backup bucket |
"""


default_arguments = {
    'owner' : 'Iyed Saadli',
    'start_date' : days_ago(1)
}

def bucket_list_objects(bucket=None, **kwargs):
    hook = GoogleCloudStorageHook()
    storage_objects = hook.list(bucket)
    kwargs['ti'].xcom_push(key='push_obj_names', value=storage_objects)

def move_objects(source_bucket=None, destination_bucket=None, prefix=None, **kwargs):
    hook = GoogleCloudStorageHook()
    #pull pushed objects
    storage_objects = kwargs['ti'].xcom_pull(key='push_obj_names', task_ids = 'list_bucket_files') # return a list of objects

    # we iterate over the list of objects
    for storage_object in storage_objects:

        destination_object = storage_object

        if prefix:
            destination_object = "{}/{}".format(prefix, storage_object)

        hook.copy(source_bucket, storage_object, destination_bucket, destination_object)
        hook.delete(source_bucket, storage_object)



with DAG(
    'move_data',
    schedule_interval = '@daily',
    default_args = default_arguments,
    catchup = False,
    max_active_runs = 1,
    user_defined_macros= {"project":"iyed-2020", "dataset":"logistic_company_etl", "principal_bucket":"depot-data"}
    ) as dag:

    list_objects = PythonOperator(
                                task_id = "list_bucket_files",
                                python_callable = bucket_list_objects,
                                op_kwargs={"bucket": "{{ principal_bucket }}"},
                                provide_context = True,
    )

    load_data = GoogleCloudStorageToBigQueryOperator(
                                task_id = 'load_to_bq',
                                bucket = "{{ principal_bucket }}",
                                source_objects = ['*'],
                                source_format = 'CSV',
                                skip_leading_rows = 1,
                                field_delimiter = ',',
                                destination_project_dataset_table = "{{ project }}.{{ dataset }}.statuscars",
                                create_disposition = 'CREATE_IF_NEEDED',
                                write_disposition = 'WRITE_TRUNCATE',
                                bigquery_conn_id = 'google_cloud_default',
                                google_cloud_storage_conn_id = 'google_cloud_default',
    )



    sql_get_latest_status = """
                 
                select * except(rank) from (SELECT *, 
                ROW_NUMBER() over (
                    partition by vehicle_id order by datetime(date, TIME(hour, minute, 0))
                    ) as rank 
                FROM `{{ project }}.{{ dataset }}.statuscars`) where rank = 1
                """
    filter_latest_status = BigQueryOperator(
                                task_id = "laststatus",
                                sql = sql_get_latest_status,
                                destination_dataset_table = "{{ project }}.{{ dataset }}.lateststatus",
                                create_disposition = 'CREATE_IF_NEEDED',
                                write_disposition = 'WRITE_TRUNCATE',
                                use_legacy_sql = False,
                                location = 'europe-north1',
                                bigquery_conn_id = 'google_cloud_default'
    )

    archivate_files = PythonOperator(
                            task_id = "deplace_files",
                            python_callable=move_objects,
                            op_kwargs= {
                                'source_bucket':'depot-data',
                                'destination_bucket':'back-depot-data',
                                'prefix': "{{ ds_nodash }}"
                            },
                            provide_context = True,
    )

list_objects >> load_data >> filter_latest_status >> archivate_files