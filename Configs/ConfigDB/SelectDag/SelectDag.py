from Configs.ConfigDB.ConnectDB.ConnectDB import ConnectDB
from Configs.ConfigConnectionAiflow.ConnectionAirflow import ConnectionAirflow

connectionairflow = ConnectionAirflow("oracle_f88_dwh")
conn = connectionairflow.get_connection_DB()

#Trả về kết quả lấy dữ liệu
def results():
    #Gọi class ConnectDB
    connectdb = ConnectDB(
        user = conn["login"], 
        password = conn["password"], 
        host = conn["host"], 
        port = conn["port"], 
        dbname = conn["schema"]
    )
    results = connectdb.select(conn["query_select"])
    return results