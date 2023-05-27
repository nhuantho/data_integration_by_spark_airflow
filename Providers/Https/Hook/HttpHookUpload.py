from typing import Any
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from Configs.ConfigHttp.HttpClass import HttpClass
from datetime import datetime, timedelta
import pandas as pd
from Configs.ConfigPaths.PathFile import setpath, cwd, removefile

class HttpHookUpload(BaseHook):
    def __init__(self, 
        conn_id, 
        report_name,
        frequent,
        folder_report
    ):
        super().__init__()
        self.conn_id = conn_id
        self.report_name = report_name
        self.frequent = frequent
        self.folder_report = folder_report
    #lấy tham số connection trên airflow
    def get_conn(self) -> Any:
        conn_data = {
            "host": "",
            "doc_library":"",
            "site_name":"",
            "login":"",
            "password":"",
        }

        try:
            conn = self.get_connection(conn_id = self.conn_id)

            conn_data["host"] = conn.host
            
            conn_data["login"] = conn.login
            conn_data["password"] = conn.get_password()
            
            extra_info = conn.extra_dejson
            conn_data["doc_library"] = extra_info["doc_library"]
            conn_data["site_name"] = extra_info["site_name"]

            return conn_data
        
        except AirflowException:
            self.log.info(
                "Could not load connection string %s, defaulting to %s", self.conn_id)
            
        return conn_data
    #Kiểm tra thời gian
    def checkstartdate(self, execution_date):
        start = datetime.strptime(str(execution_date)[0:10], "%Y-%m-%d")
        if self.frequent == 'daily':
            start = start + timedelta(days= -1)
        elif self.frequent == 'weekly':
            start = start + timedelta(days= -7)
        elif self.frequent == 'monthly':
            start = start - pd.DateOffset(months=1)
        return start
    #Đọc file dữ liệu
    def readfile(self, file_path):
        with open(file_path, 'rb') as f:
            return f.read()
    
    def httpupload(self, execution_date):
        conn = self.get_conn()
        day1 = self.checkstartdate(execution_date)
        #Cắt ngày tháng năm
        date = str(day1)[0:10]
        day = "/" + date[8:10]
        month = "/" + date[5:7]
        year = "/" + date[0:4]
        if self.frequent == 'monthly':
            day = ""
        #Set lại đường dẫn
        path = setpath(self.folder_report, year, month, day)
        path_file = setpath(cwd, "/Data/", path, self.report_name)
        content = self.readfile(path_file)
        #Gọi class http class và upload file
        httpclass = HttpClass(
            user=conn["login"],
            password=conn["password"], 
            url_site=conn["host"], 
            site_name=conn["site_name"], 
            doc_library=conn["doc_library"],
            file_name=self.report_name,
            folder_name=path,
            content=content
        )
        httpclass.uploadfile()
        #Xóa file khi đã đẩy lên sharepoint
        removefile(path_file)
