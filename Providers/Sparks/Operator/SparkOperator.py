from typing import Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from Providers.Sparks.Hook.SparkHook import SparkHook

class SparkOperator(BaseOperator):
    def __init__(self,
        task_id,
        conn_id,
        query, 
        report_name,
        frequent,
        folder_report,
        cores,
        memory, 
        **kwargs
    ):
        super().__init__(**kwargs, task_id=task_id)
        self.conn_id = conn_id
        self.query = query
        self.report_name = report_name
        self.frequent = frequent
        self.folder_report = folder_report
        self.cores = cores
        self.memory = memory
    #Gọi spark hook
    def _gethook(self) -> SparkHook:
        return SparkHook(
            conn_id = self.conn_id,
            query = self.query, 
            report_name = self.report_name,
            frequent = self.frequent,
            folder_report = self.folder_report,
            cores = self.cores,
            memory = self.memory
        )
    #Thực thi khi gọi operator
    def execute(self, context: Context) -> Any:
        #Lấy ngày dag chạy
        execution_date = context["dag_run"].logical_date
        sparkhook = self._gethook()
        #Thực thi câu lệnh
        sparkhook.sparkrun(execution_date=execution_date, dag_id=self.task_id)