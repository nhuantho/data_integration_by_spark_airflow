from ConnectDB import ConnectDB
from dotenv import load_dotenv
from pathlib import Path
from DataDags import data, listdata
import os

#Xử lý đường dẫn từ /home/nhuan/Documents/Airflow_project/data_integration_by_spark/Configs/ToolDB/ConnectDB -> 
#/home/nhuan/Documents/Airflow_project/data_integration_by_spark
cwd = os.getcwd()
cwd = os.path.dirname(cwd)
cwd = os.path.dirname(cwd)
cwd = os.path.dirname(cwd)

#Tải các biến trong file .env
dotenv_path = Path(cwd + '/.env')
load_dotenv(dotenv_path=dotenv_path)

#Lấy thông tin dữ liệu từ file .env
user = os.getenv('USERDB')
password = os.getenv('PASSWORD')
host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DBNAME')
query = os.getenv('QUERYINSERT')

#Gọi class ConnectDB
connectdb = ConnectDB(user, password, host, port, dbname)

#Thêm một hàng dữ liệu
def insertone(query, data, user, password, host, port, dbname):
    connectdb = ConnectDB(user, password, host, port, dbname)
    connectdb.insert(query, data)

#Thêm nhiều hàng dữ liệu
def insertmany(query, data, user, password, host, port, dbname):
    connectdb = ConnectDB(user, password, host, port, dbname)
    connectdb.insertmany(query, data)

#Thực thi câu lệnh thêm dữ liệu
insertmany(query, listdata(), user, password, host, port, dbname)