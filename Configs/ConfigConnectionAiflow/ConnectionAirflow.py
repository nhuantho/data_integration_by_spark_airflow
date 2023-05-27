from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import Connection #dùng để lấy các tham số trong connect

class ConnectionAirflow:
    def __init__(self, connection_id: str = "spark_default"):
        self.connection_id = connection_id
    
    #Lấy các giá trị connection của airflow
    def get_connection_spark(self) -> Any:
        conn_data = {
            "master": "",
            "url":"",
            "drive":"",
            "login":"",
            "password":"",
        }
        try:
            conn = Connection.get_connection_from_secrets(conn_id = self.connection_id)

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
                "Could not load connection string %s, defaulting to %s", self.connection_id)
    
    def get_connection_DB(self):
        conn_data = {
            "host": "",
            "schema":"",
            "login":"",
            "password":"",
            "port":"",
            "query_select":"",
        }
        try:
            conn = Connection.get_connection_from_secrets(conn_id = self.connection_id)
            
            conn_data["host"] = conn.host
            conn_data["schema"] = conn.schema
            conn_data["login"] = conn.login
            conn_data["password"] = conn.get_password()
            conn_data["port"] = conn.port

            extra_info = conn.extra_dejson
            conn_data["query_select"] = extra_info["query_select"]

            return conn_data
        except AirflowException:
            self.log.info(
                "Could not load connection string %s, defaulting to %s", self.connection_id)
    
        