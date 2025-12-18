FROM eclipse-temurin:11-jre-jammy

# Install Python and wget
RUN apt-get update && apt-get install -y python3 python3-pip wget && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.0-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.4.0-bin-hadoop3 /opt/spark && \
    rm spark-3.4.0-bin-hadoop3.tgz

ENV PATH="/opt/spark/bin:${PATH}"
ENV HOME=/tmp
ENV SPARK_HOME=/opt/spark

WORKDIR /opt/spark/app

COPY spark_streaming.py /opt/spark/app/

# CORRECT (Exec form - uses brackets):
ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0"]

# This appends to the entrypoint:
CMD ["spark_streaming.py"]