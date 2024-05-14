FROM apache/spark-py:v3.4.0 AS pyspark

# Don't use pip as root like a good boy
USER root
RUN adduser --uid 1000 --disabled-password jay
USER jay

# Install python deps with few cache invalidations
COPY requirements/base.txt /tmp/requirements.txt
RUN pip install --requirement /tmp/requirements.txt
COPY . /tmp/

# Setup Spark related environment variables
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# spark-defaults.conf take lowest precedence behind any submit flags
COPY conf/spark-defaults.conf "$SPARK_HOME/conf/"

USER root
WORKDIR ${SPARK_HOME}
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copy appropriate entrypoint script and make executable
COPY entrypoint.sh .
RUN chmod u+x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
