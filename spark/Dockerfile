FROM bitnami/spark:3.5

USER root

# Install required packages
RUN apt-get update && apt-get install -y wget curl

# Download S3A dependencies
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Create non-root user for Spark to avoid Ivy home issues
RUN useradd -ms /bin/bash sparkuser \
    && chown -R sparkuser:sparkuser /opt/bitnami /home/sparkuser

# Switch to sparkuser
USER sparkuser

# Set HOME so Ivy basedir is absolute
ENV HOME=/home/sparkuser
WORKDIR /home/sparkuser
