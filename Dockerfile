FROM r-base:4.0.3
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends git libcurl4-openssl-dev libssl-dev libxml2-dev build-essential libpq-dev python3 python3-pip python3-setuptools python3-dev
# RUN pip3 install --upgrade pip

ENV PYTHONPATH "${PYTHONPATH}:/app"
WORKDIR /app

ADD requirements.txt .
ADD requirements.r .

# installing python libraries
RUN pip3 install --break-system-packages -r requirements.txt

# installing r libraries
RUN Rscript requirements.r

COPY results_collector.py /
COPY reporter.py /
COPY data_manager.py /
COPY results_processor.R /
COPY start.sh /
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/start.sh"]