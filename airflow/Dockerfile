# Kế thừa từ image Airflow gốc mà bạn đang dùng trong docker-compose.
# Hãy đảm bảo phiên bản này khớp với phiên bản trong file docker-compose.yaml của bạn.
FROM apache/airflow:3.0.1

# Chuyển sang user airflow (user mặc định trong container để chạy các tác vụ)
USER airflow

# Cài đặt dbt-core và adapter cho BigQuery bằng pip.
# Chỉ định phiên bản cụ thể giúp đảm bảo môi trường của bạn luôn ổn định.
RUN pip install --no-cache-dir dbt-core==1.7.9 dbt-bigquery==1.7.5