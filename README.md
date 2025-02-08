# Data_Integration_by_Spark

## Cài đặt trong dự án

### Cài đặt oracle
Tải gói oracle từ trang chủ
Giải nén và chuyển sang thư mục /opt/oracle/
Đường dẫn oracle:
export LD_LIBRARY_PATH=/opt/oracle/phiên bản oracle tải về:$LD_LIBRARY_PATH
(Ví dụ: export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_10:$LD_LIBRARY_PATH)

### Tạo môi trường ảo
python3 -m venv venv

### Sử dụng môi trường ảo
source ./venv/bin/activate

### Tải thư viện vào môi trường ảo
pip install -r requirements.txt

### Cài đặt AIRFLOW trong dự án

#### Tạo AIRFLOW_HOME
export AIRFLOW_HOME=đường đẫn folder lưu trữ dự án
ví dụ(export AIRFLOW_HOME=/home/nhuan/Documents/Airflow_project/)

#### Tạo DB ở local
airflow db init

#### Tạo tài khoản Admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@gamil.com

#### Sửa airflow.cfg
dags_folder = đường đẫn folder lưu trữ dự án/data_integration_by_spark
ví dụ(dags_folder = /home/nhuan/Documents/Airflow_project/data_integration_by_spark)

default_timezone = Asia/Ho_Chi_Minh

#### Chạy dự webserver
airflow webserver --port 8012

#### Chạy lịch
airflow scheduler

## Thông tin về folder

### data_integration_by_spark/Configs
Xử lý tất cả các vấn đề liên quan đến cấu hình

#### data_integration_by_spark/Configs/ToolDB
Xử lý dữ liệu liên quan đến cơ sở dữ liệu: lấy ra dữ liệu và thêm dữ liệu dag mới

##### data_integration_by_spark/Configs/ToolDB/ConnectDB
Xử lý thông tin liên quan đến kết nối cơ sở dữ liệu lấy dữ liệu Dag từ bảng lưu thông tin về Dag
- ConnectDB.py: Là lớp thực thi đến cơ sở dữ liệu

#### data_integration_by_spark/Configs/Sparks
Xử  lý dữ liệu spark
