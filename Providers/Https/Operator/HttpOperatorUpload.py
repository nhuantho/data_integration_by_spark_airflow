from typing import Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from Providers.Https.Hook.HttpHookUpload import HttpHookUpload

class HttpOperatorUpload(BaseOperator):
    def __init__(self, 
        task_id, 
        conn_id,
        report_name,
        frequent,
        folder_report,
        **kwargs
    ):
        super().__init__(**kwargs, task_id=task_id)
        self.conn_id = conn_id
        self.report_name = report_name
        self.frequent = frequent
        self.folder_report = folder_report
    
    #Gọi lớp htttphookupload
    def _gethook(self) -> HttpHookUpload:
        return HttpHookUpload(
            conn_id = self.conn_id,
            report_name = self.report_name,
            frequent = self.frequent,
            folder_report = self.folder_report,
        )

    #Thực thi các câu lệnh khi gọp operator
    def execute(self, context: Context) -> Any:
        #Lấy ngày chạy dag
        execution_date = context["dag_run"].logical_date
        httphook = self._gethook()
        httphook.httpupload(execution_date)