FROM apache/airflow:2.8.1-python3.11

USER root

# Java + utilitaires systeme
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configuration JAVA_HOME pour ARM/AMD
RUN ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3

# Jars S3A pour que Spark parle a MinIO sans telechargement au runtime.
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar
