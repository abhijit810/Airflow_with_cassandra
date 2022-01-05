FROM apache/airflow

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir plyvel && \
    pip install --no-cache-dir nibabel pydicom matplotlib pillow && \
    pip install --no-cache-dir med2image && \
    pip install --no-cache-dir apache-airflow-providers-amazon && \
    pip install --no-cache-dir cassandra-driver && \
    pip install --no-cache-dir apache-airflow-providers-apache-cassandra && \
    pip install --no-cache-dir apache-airflow-providers-http && \
    pip install --no-cache-dir cassandra-sigv4

# test if cassandra is running
# docker exec -it cassandra bash -c "cqlsh -u cassandra -p cassandra

CMD ["cat", "/etc/os-release"]