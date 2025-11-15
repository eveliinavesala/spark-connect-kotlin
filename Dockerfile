# Use a base image with Java 17, which is required for Spark 4.0
FROM eclipse-temurin:17-jdk

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV SPARK_NO_DAEMONIZE=1

# Install curl to download Spark
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download and extract a pre-release version of Spark 4.0
# This is a preview version, as official 4.0 releases are not yet available.
RUN curl -o /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-4.0.0-preview1/spark-4.0.0-preview1-bin-hadoop3.tgz" && \
    tar -xvzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-4.0.0-preview1-bin-hadoop3 $SPARK_HOME && \
    rm /tmp/spark.tgz

# Expose the default Spark Connect port
EXPOSE 15002

# Set the command to start the Spark Connect server in local mode.
# SPARK_NO_DAEMONIZE=1 ensures the server runs in the foreground.
CMD ["/opt/spark/sbin/start-connect-server.sh", "--master", "local[*]"]
