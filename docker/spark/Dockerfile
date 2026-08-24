# Single image shared by spark-master, spark-worker and the pipeline driver.
# All three need the S3A connector jars: the driver to plan the job, the
# executors to actually read/write s3a:// paths on MinIO.
FROM apache/spark:3.5.1

USER root

# S3A connector. Versions must match the Hadoop version bundled in the image
# (hadoop-client-*-3.3.4.jar), hence hadoop-aws 3.3.4 + its matching AWS SDK.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
RUN curl -fsSL -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
 && curl -fsSL -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# PostgreSQL JDBC driver, needed by the executors to write the warehouse.
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN curl -fsSL -o "${SPARK_HOME}/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY run_pipeline.sh /app/run_pipeline.sh
RUN chmod +x /app/run_pipeline.sh

ENV PATH="/opt/spark/bin:${PATH}"

# No ENTRYPOINT override: the base image entrypoint passes the command
# through, so each service supplies its own command in docker-compose.yml.
