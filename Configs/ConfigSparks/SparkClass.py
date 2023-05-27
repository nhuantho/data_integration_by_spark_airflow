from pyspark.sql import SparkSession
import pandas as pd
# import databricks.koalas as ks
import pyspark.pandas as spd
from Configs.ConfigPaths.PathFile import cwd, setpath, checkpath, removefile
from pyexcelerate import Workbook
import os

#Khởi tạ lớp spark
class SparkClass:
    def __init__(
        self,
        appname,
        query, 
        day1,
        day2, 
        report_name,
        frequent,
        folder_report,
        master,
        url,
        drive,
        user,
        password,
        cores,
        memory
    ):
        self.appname = appname
        self.query = query
        self.day1 = day1
        self.day2 = day2
        self.report_name = report_name
        self.frequent = frequent
        self.folder_report = folder_report
        self.master = master
        self.url = url
        self.drive = drive
        self.user = user
        self.password = password
        self.cores = cores
        self.memory = memory

    #Khởi tạo spark
    def _spark(self):
        spark = SparkSession.builder\
                .appName(self.appname)\
                .master(self.master) \
                .getOrCreate()
        return spark

    #Xử lý câu query về định dạng chuẩn
    def _formatquery(self):
        day1=str(self.day1)
        day2 = str(self.day2)
        query = self.query
        day1 = '\'' + day1[0:10].replace('-', '') + '\''
        day2 = '\'' + day2[0:10].replace('-', '') + '\''
        query = query.replace(':DAY1', day1)
        query = query.replace(':DAY2', day2)
        query=query.replace('0x91', '\'')
        query=query.replace('0x93', '\"')
        return query
    
    #Load dữ liệu từ data warehouse ra dataframe
    def _loaddata(self, query):
        spark = self._spark()
        dataframe = spark.read.format("jdbc")\
        .option("url", self.url)\
        .option("drive", self.drive)\
        .option("query", query)\
        .option("user", self.user)\
        .option("password", self.password)\
        .load()
        return dataframe
    
    #Hàm ghi dữ liệu vào sheet
    def _writesheet(self, query, sheet, workbook):
        dataframe = self._loaddata(query)
        df = spd.DataFrame(dataframe).astype(str)
        # Tạo sheet mới
        sheet = workbook.new_sheet(sheet)

        # Ghi tiêu đề cột
        column_headers = list(df.columns)
        num_columns = len(column_headers)
        header_range = sheet.range((1, 1), (1, num_columns))
        header_range.value = [column_headers]

        # Ghi dữ liệu từ DataFrame
        data = df.values.tolist()
        num_rows = len(data)
        data_range = sheet.range((2, 1), (num_rows + 1, num_columns))
        data_range.value = data
    
    #Viết dữ liệu ra file
    def writefile(self):
        #Đường dẫn lưu file
        date = str(self.day1)[0:10]
        day = "/" + date[8:10]
        month = "/" + date[5:7]
        year = "/" + date[0:4]
        if self.frequent == 'monthly':
            day = ""
        path = setpath(cwd, "/Data", self.folder_report, year, month, day)
        #Kiểm tra thư mục
        checkpath(path)

        path = setpath(path, self.report_name)
        #Kiểm tra file
        removefile(path)
        #Cắt file theo --SPLIT
        queries = self._formatquery().split('--SPLIT')
        #Khởi tạo workbook
        workbook = Workbook()
        #Ghi nhiều sheet vào workbook
        for i in range(len(queries)):
            self._writesheet(query=queries[i], sheet="sheet " + str(i + 1), workbook=workbook)
        #Lưu file
        workbook.save(path)

        