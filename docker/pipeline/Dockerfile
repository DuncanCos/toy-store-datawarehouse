FROM apache/spark:3.5.1

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY run_pipeline.sh /app/run_pipeline.sh
RUN chmod +x /app/run_pipeline.sh

ENV PATH="/opt/spark/bin:${PATH}"

ENTRYPOINT ["/app/run_pipeline.sh"]
