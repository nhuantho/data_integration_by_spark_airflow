from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from Providers.Sparks.Operator.SparkOperator import SparkOperator
from Providers.Https.Operator.HttpOperatorUpload import HttpOperatorUpload
from Configs.ConfigDB.SelectDag.SelectDag import results
from Configs.ConfigPaths.PathFile import setpath

start_date = start_date = datetime(2023, 5, 8)

dags = results()

for dag in dags:
    with DAG(
            dag_id=dag[0],
            start_date=start_date,
            schedule_interval=dag[4],
            description=dag[9],
            default_args={'owner': 'F88-DE'},
            catchup=True,
            tags=dag[3].split(',')
    ) as d:
        start = EmptyOperator(task_id="Start")

        sparksubmit = SparkOperator(
            task_id = dag[0] + "sparksubmit",
            conn_id="spark_f88",
            query=dag[8].read(),
            report_name=dag[2],
            frequent=dag[5],
            folder_report=setpath("/" + dag[6], "/" + dag[1]),
            cores="4",
            memory="4"
        )

        httpupload = HttpOperatorUpload(
            task_id=dag[0] + "httpupload",
            conn_id="http_f88",
            report_name=dag[2],
            frequent=dag[5],
            folder_report=setpath(dag[6], "/" + dag[1])
        )

        end = EmptyOperator(task_id="End")
        start >> sparksubmit >> httpupload >> end