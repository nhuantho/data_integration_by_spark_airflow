from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag_ids = ["OFFSHORE_DAILY_LENDABLE_COLLECTION", "TLS_DAILY_DATA_DETAIL_POL"]

with DAG (
    dag_id='DAGMAIN',
    start_date=datetime(2023, 5, 8),
    schedule_interval=None
) as d:
    start = EmptyOperator(task_id="Start")
    list_task = []
    for dag_id in dag_ids:
        trigger_operator = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
        )
        list_task.append(trigger_operator)
    end = EmptyOperator(task_id="End")
    start >> list_task >> end
