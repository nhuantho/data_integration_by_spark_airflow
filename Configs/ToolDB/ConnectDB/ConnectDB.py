import cx_Oracle

class ConnectDB:
    #Hàm khởi tạo các thuộc tính sử dụng
    def __init__(
        self,
        user,
        password,
        host,
        port,
        dbname
    ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname

    #Hàm kết nối đến cơ sở dữ liệu
    def _connect(self):
        connection = cx_Oracle.connect(
            user=self.user, 
            password=self.password, 
            dsn='{host}:{port}/{dbname}'.format(host = self.host, port = self.port, dbname = self.dbname)
        )
        return connection
    
    #Hàm thêm một thực thể (hàng dữ liệu) vào cơ sở dữ liệu
    def insert(self, query, data):
        try:
            connection = self._connect()
            cursor = connection.cursor()
            cursor.execute(query, data)
            connection.commit()
            cursor.close()
            connection.close()
        except cx_Oracle.DatabaseError as e:
            print(e)

    #Hàm thêm tập thực thể (nhiều hàng dữ liệu) vào cơ sở dữ liệu
    def insertmany(self, query, data):
        try:
            connection = self._connect()
            cursor = connection.cursor()
            cursor.executemany(query, data)
            connection.commit()
            cursor.close()
            connection.close()
        except cx_Oracle.DatabaseError as e:
            print(e)

    #Hàm lấy dữ liệu từ cơ sở dữ liệu
    def select(self, query):
        connection = self._connect()
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results



