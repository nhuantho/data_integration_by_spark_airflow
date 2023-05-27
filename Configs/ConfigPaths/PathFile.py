import os

#lấy đường dẫn thư mục
cwd = os.getcwd() + "/data_integration_by_spark"

#Set lại đường dẫn file
def setpath(*args):
    path = ""
    for arg in args:
        path = path + arg
    return path

#Kiểm tra folder có tồn tại không, nếu chưa có thì tạo mới
def checkpath(path):
    if os.path.isdir(path) == False:
        os.makedirs(path)

#Kiểm tra file có tồn tại không, nếu đã có thì xóa đi
def removefile(path):
    if os.path.isfile(path):
        os.remove(path)

