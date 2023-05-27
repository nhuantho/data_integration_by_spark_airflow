from typing import Any, TYPE_CHECKING
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from Configs.ConfigSparks.SparkClass import SparkClass
from datetime import datetime, timedelta
import pandas as pd

class SparkHook(BaseHook):
    def __init__(self,
        conn_id,
        query, 
        report_name,
        frequent,
        folder_report,
        cores,
        memory
    ):
        super().__init__()
        self.conn_id = conn_id
        self.query = query
        self.report_name = report_name
        self.frequent = frequent
        self.folder_report = folder_report
        self.cores = cores
        self.memory = memory
    #Lấy tham số connetion trong airflow
    def get_conn(self) -> Any:
        conn_data = {
            "master": "",
            "url":"",
            "drive":"",
            "login":"",
            "password":"",
        }
        try:
            conn = self.get_connection(conn_id = self.conn_id)

            if conn.port:
                conn_data["master"] = f"{conn.host}:{conn.port}"
            else:
                conn_data["master"] = conn.host
            
            conn_data["login"] = conn.login
            conn_data["password"] = conn.get_password()
            
            extra_info = conn.extra_dejson
            conn_data["url"] = extra_info["url"]
            conn_data["drive"] = extra_info["drive"]

            return conn_data
        
        except AirflowException:
            self.log.info(
                "Could not load connection string %s, defaulting to %s", self.conn_id)
            
        return conn_data
    #Xử lý ngày
    def checkstartdate(self, execution_date):
        start = datetime.strptime(str(execution_date)[0:10], "%Y-%m-%d")
        if self.frequent == 'daily':
            start = start + timedelta(days= -1)
        elif self.frequent == 'weekly':
            start = start + timedelta(days= -7)
        elif self.frequent == 'monthly':
            start = start - pd.DateOffset(months=1)
        return start
    #Thực thi class
    def sparkrun(self, execution_date, dag_id):
        conn = self.get_conn()
        #Gọi hàm xử lý ngày
        day1 = self.checkstartdate(execution_date)
        day2 = execution_date
        #Gọi spark class
        sparkclass = SparkClass(
            appname=dag_id,
            query=self.query,
            day1=day1,
            day2=day2,
            report_name=self.report_name,
            frequent=self.frequent,
            folder_report=self.folder_report,
            master=conn["master"],
            url=conn["url"],
            drive=conn["drive"],
            user=conn["login"],
            password=conn["password"],
            cores=self.cores,
            memory=self.memory
        )
        #Viết file
        sparkclass.writefile()