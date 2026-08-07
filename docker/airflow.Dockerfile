FROM apache/airflow:3.3.0-python3.11

COPY docker/airflow-requirements.txt /tmp/airflow-requirements.txt

# Airflow 버전을 다시 명시해 추가 패키지가 Airflow를 암묵적으로
# 업그레이드하거나 다운그레이드하지 못하도록 한다.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/airflow-requirements.txt
