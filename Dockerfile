# Use a base image with Java 21, which is supported by Spark 4.0
FROM eclipse-temurin:21-jdk

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV SPARK_NO_DAEMONIZE=1

# Create a dedicated non-root user and group
RUN groupadd -r spark && useradd -r -g spark spark

# Install curl to download Spark
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download and extract Spark 4.0.0, then transfer ownership of SPARK_HOME
RUN curl -o /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz" && \
    tar -xvzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-4.0.0-bin-hadoop3 $SPARK_HOME && \
    rm /tmp/spark.tgz \
    && chown -R spark:spark $SPARK_HOME

# Prepare the application directory with correct permissions
WORKDIR /app
RUN chown spark:spark /app
COPY --chown=spark:spark . .

# Drop privileges before exposing ports or running any process
USER spark

# Expose the default Spark Connect port
EXPOSE 15002

# Set the command to start the Spark Connect server in local mode.
# SPARK_NO_DAEMONIZE=1 ensures the server runs in the foreground.
CMD ["/opt/spark/sbin/start-connect-server.sh", "--master", "local[*]"]
