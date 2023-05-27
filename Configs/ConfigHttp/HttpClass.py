from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

#Khởi tạo class http để dùng các phương thức upload file, download file
class HttpClass:
    def __init__(self, 
        user, 
        password, 
        url_site, 
        site_name, 
        doc_library, 
        file_name,
        folder_name,
        content
    ):
        self.user = user
        self.password = password
        self.url_site = url_site
        self.site_name = site_name
        self.doc_library = doc_library
        self.file_name = file_name
        self.folder_name = folder_name
        self.content = content

    #Kết nối đến sharepoint
    def _auth(self):
        conn = ClientContext(self.url_site).with_credentials(
            UserCredential(
                user_name=self.user,
                password=self.password
            )
        )
        return conn

    #Tải dữ liệu lên sharepoint
    def uploadfile(self):
        conn = self._auth()
        target_folder_url = f'{self.doc_library}/{self.folder_name}'
        #Kiểm tra thư mục có tồn tại hay không, nếu chưa có thì thêm mới
        target_folder = conn.web.ensure_folder_path(target_folder_url)
        #Upload file lên sharepoint
        response = target_folder.upload_file(self.file_name[1:], self.content).execute_query()
        return response

# def upload_file(folder, **context):
#     execution_date = context['execution_date']
#     dag_id = context['dag'].dag_id
#     path = cwd + dagdetail[dag_id]["report_name"].format(date = str(execution_date)[0:10])
#     content = get_file_content(path)
#     SharePoint().upload_file(path.split('/')[-1], folder, content)
#     if os.path.isfile(path):
#         os.remove(path)

# def get_file_content(file_path):
#     with open(file_path, 'rb') as f:
#         return f.read()
