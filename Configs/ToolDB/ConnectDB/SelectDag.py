from ConnectDB import ConnectDB
from dotenv import load_dotenv
from pathlib import Path
import os
#Tải các biến trong file .env
dotenv_path = Path('/home/nhuan/Documents/Airflow_project/data_integration_by_spark/.env')
load_dotenv(dotenv_path=dotenv_path)

#Lấy thông tin dữ liệu từ file .env
user = os.getenv('USERDB')
password = os.getenv('PASSWORD')
host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DBNAME')
query = os.getenv('QUERYSELECT')

#Trả về kết quả lấy dữ liệu
def results():
    #Gọi class ConnectDB
    connectdb = ConnectDB(user, password, host, port, dbname)
    results = connectdb.select(query)
    return results

result = results()
for i in result:
    file = open(i[0]+".txt", "wb")
    file.write(i[8].read().encode("utf-8"))

