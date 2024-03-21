from datetime import datetime, date
import os
import json
import boto3


from airflow.decorators import (
    dag,
    task,
    task_group
) 
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.lambda_function import LambdaFunctionStateSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator


DAG_NAME = 'nyc_data_pipeline'
default_args = {
    'start_date': datetime(2024, 1, 1),
}
@dag(schedule_interval=None, default_args=default_args, catchup=False)
def nyc_data_pipeline():

    keys ={
        "vehicle": "bm4k-52h4",
        "crashes": "h9gi-nx95",
        "person": "f55k-p6yu"
    }

    base_payload_dict = {
        "domain": "data.cityofnewyork.us",
        "bucket-name": "nyc-data-landing-zone",
        "secret-name": "Jawahar/access_keys",
        "region-name": "us-east-1",
        "timeout": "10",
    }

    success_tasks = []

    for key, dataset_key in keys.items():
        payload_dict = base_payload_dict.copy()
        payload_dict["dataset-key"] = dataset_key
        payload_dict["folder-name"] = key
        today_str = date.today().strftime("%Y-%m-%d")
        payload_dict["file-name"] = f"{today_str}_{key}.csv"
        payload_json = json.dumps(payload_dict)

        @task_group(group_id=f'lambda_task_group_{key}')
        def create_lambda_task_group(key, dataset_key):        
            invoke_lambda_function = LambdaInvokeFunctionOperator(
                task_id=f"invoke_lambda_function_{key}",
                function_name="nyc-extract-lambda-handler",
                aws_conn_id="aws_conn",
                region_name="us-east-1",
                payload=payload_json
            )

            @task.branch
            def branch_based_on_lambda_status_code(lambda_state: str):
                lambda_state = json.loads(lambda_state)
                if lambda_state:
                    if  lambda_state.get('statusCode') == 200:
                        print("Lambda function executed successfully.")
                        return f'lambda_task_group_{key}.s3_upload_{key}'
                    else:
                        return f'lambda_task_group_{key}.lambda_failure_notify_{key}'

            lambda_success_function = DummyOperator(
                task_id=f's3_upload_{key}'
            )

            lambda_failure_notify = DummyOperator(
                task_id=f'lambda_failure_notify_{key}'
            )

            branch_task_id = branch_based_on_lambda_status_code(invoke_lambda_function.output)
            branch_task_id  >> [lambda_success_function, lambda_failure_notify]

            return lambda_success_function
        success_tasks.append(create_lambda_task_group(key, dataset_key))

    
    final_task = DummyOperator(
        task_id='final_task',
        trigger_rule='all_done'
    )

    for success_task in success_tasks:
        success_task >> final_task

dag = nyc_data_pipeline()
