#Thông tin một dag, cần đưa thông tin Dag về định dạng dict theo mẫu 
dag = {
        "DAG_ID":"TLS_DAILY_LEAD_PDG1", 
        "FOLDER":"D_TLS", 
        "REPORT_NAME":"/BC LEAD PGD.xlsx", 
        "TAGS":"mkt,daily,lead_pgd", 
        "SCHEDULE_INTERVAL":"0 10 * * *", 
        "FREQUENT":"daily", 
        "TYPE_":"Data Warehouse to SharePoint - Report F88", 
        "DEPARMENT":"tls", 
        "QUERY":"./QueryFiles/LEAD_PGD.sql", 
        "DESCRIPTION":"BC LEAD PGD", 
        "STATUS":1
    }

#Thông tin nhiều dag, cần đưa thông tin Dags về định dạng dict theo mẫu 
dags = {
    "TLS_DAILY_DATA_DETAIL_POL":{
        "DAG_ID":"TLS_DAILY_DATA_DETAIL_POL", 
        "FOLDER":"D_TLS", 
        "REPORT_NAME":"/Data chi tiết đơn Online daily - TuyenDN.xlsx", 
        "TAGS":"tls,daily", 
        "SCHEDULE_INTERVAL":"0 10 * * *", 
        "FREQUENT":"daily", 
        "TYPE_":"Data Warehouse to SharePoint - Report F88", 
        "DEPARMENT":"tls", 
        "QUERY":"./QueryFiles/DATA_DETAIL_POL.sql", 
        "DESCRIPTION":"Báo cáo Data chi tiết đơn Online daily - TuyenDN", 
        "STATUS":1
    },
    "TLS_DAILY_LEAD_PDG":{
        "DAG_ID":"TLS_DAILY_LEAD_PDG", 
        "FOLDER":"D_TLS", 
        "REPORT_NAME":"/BC LEAD PGD.xlsx", 
        "TAGS":"mkt,daily,lead_pgd", 
        "SCHEDULE_INTERVAL":"0 10 * * *", 
        "FREQUENT":"daily", 
        "TYPE_":"Data Warehouse to SharePoint - Report F88", 
        "DEPARMENT":"tls", 
        "QUERY":"./QueryFiles/LEAD_PGD.sql", 
        "DESCRIPTION":"BC LEAD PGD", 
        "STATUS":1
    },
    "TLS_DAILY_NEW_CONTRACT":{
        "DAG_ID":"TLS_DAILY_NEW_CONTRACT", 
        "FOLDER":"D_TLS", 
        "REPORT_NAME":"/Báo cáo giải ngân của TLS/Báo cáo chi tiết Hợp đồng mở mới.xlsx", 
        "TAGS":"tls,daily", 
        "SCHEDULE_INTERVAL":"0 10 * * *", 
        "FREQUENT":"daily", 
        "TYPE_":"Data Warehouse to SharePoint - Report F88", 
        "DEPARMENT":"tls", 
        "QUERY":"./QueryFiles/NEW_CONTRACT.sql", 
        "DESCRIPTION":"Báo cáo Báo cáo giải ngân của TLS/Báo cáo chi tiết Hợp đồng mở mới daily - TuyenDN", 
        "STATUS":1
    },
    "TLS_DAILY_TDTD":{
        "DAG_ID":"TLS_DAILY_TDTD", 
        "FOLDER":"D_TLS", 
        "REPORT_NAME":"/báo cáo TDTD.xlsx", 
        "TAGS":"tls,daily", 
        "SCHEDULE_INTERVAL":"0 10 * * *", 
        "FREQUENT":"daily", 
        "TYPE_":"Data Warehouse to SharePoint - Report F88", 
        "DEPARMENT":"tls", 
        "QUERY":"./QueryFiles/TDTD.sql", 
        "DESCRIPTION":"", 
        "STATUS":1
    },
    "PTKD_MONTHLY_MONTHLY_OUTSTANDING":{
        "DAG_ID":"PTKD_MONTHLY_MONTHLY_OUTSTANDING", 
        "FOLDER":"M_PTKD", 
        "REPORT_NAME":"/Monthly Outstanding report.xlsx", 
        "TAGS":"ptkd,monthly", 
        "SCHEDULE_INTERVAL":"0 10 1 * *", 
        "FREQUENT":"monthly", 
        "TYPE_":"Data Warehouse to SharePoint - Report F88", 
        "DEPARMENT":"ptkd", 
        "QUERY":"./QueryFiles/MONTHLY_OUTSTANDING.sql", 
        "DESCRIPTION":"Báo cáo Monthly Outstanding report monthly - TuyenDN", 
        "STATUS":1
    },
    "PTKD_WEEKLY_POL_SHOP":{
        "DAG_ID":"PTKD_WEEKLY_POL_SHOP", 
        "FOLDER":"W_PTKD", 
        "REPORT_NAME":"/Số ngày PGD gọi đơn POL.xlsx", 
        "TAGS":"ptkd,weekly", 
        "SCHEDULE_INTERVAL":"0 10 * * 0", 
        "FREQUENT":"weekly", 
        "TYPE_":"Data Warehouse to SharePoint - Report F88", 
        "DEPARMENT":"ptkd", 
        "QUERY":"./QueryFiles/POL_SHOP.sql", 
        "DESCRIPTION":"Báo cáo Số ngày PGD gọi đơn POL weekly - TuyenDN", 
        "STATUS":1
    },
    "OFFSHORE_DAILY_LENDABLE_COLLECTION":{
        "DAG_ID":"OFFSHORE_DAILY_LENDABLE_COLLECTION", 
        "FOLDER":"D_OFFSHORE", 
        "REPORT_NAME":"/LENDABLE_COLLECTION.xlsx", 
        "TAGS":"offshore,daily,lendable", 
        "SCHEDULE_INTERVAL":"0 10 * * *", 
        "FREQUENT":"daily", 
        "TYPE_":"Data Warehouse to SharePoint - Report F88", 
        "DEPARMENT":"offshore", 
        "QUERY":"./QueryFiles/LENDABLE_COLLECTION.sql", 
        "DESCRIPTION":"Báo cáo lendable collection - NGUYENNT", 
        "STATUS":1
    }
}

#Hàm định dạng query, đưa các đối số  cần thêm trong câu query về một định dạng chung
def formatquery(query):
    query = query.upper()
    query=query.replace(':DATE_WID', ':DAY1')
    query=query.replace('\'', '0x91')
    query=query.replace('\"', '0x93')
    query=query.replace('\n', ' ')
    query=' '.join(query.split())
    return query

#Đọc file dữ liệu, file dữ liệu cần được thêm vào QueryFile trước thực thi
def readfile(file):
    f = open(file, "r")
    queryinsert = f.read()
    return queryinsert

#Đưa dữ liệu về dạng tuple
def formatdata(dag, filequery):
    query = formatquery(readfile(filequery))
    data = []
    for i in dag.keys():
        if i != 'QUERY':
            data.append(dag[i])
        elif i == 'QUERY':
            data.append(query)
    return tuple(data)

#Trả về một tuple dữ liệu
def data():
    return formatdata(dag, dag['QUERY'])

#Trả về một danh sách tuple dữ liệu
def listdata():
    data = []
    for dag in dags.values():
        data.append(formatdata(dag, dag['QUERY']))
    return data