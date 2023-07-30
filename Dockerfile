FROM r-base:4.3.1
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt upgrade -y
# RUN apt-get install -y --no-install-recommends git libcurl4-openssl-dev libssl-dev libxml2-dev build-essential libpq-dev python3 python3-pip python3-setuptools python3-dev
RUN apt install -y git libcurl4-openssl-dev libssl-dev libxml2-dev build-essential libpq-dev python3.11-venv python3-setuptools python3-dev
WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/app/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python -m pip install --upgrade pip

## installing python libraries
COPY requirements.txt .
RUN pip install -r requirements.txt


# installing r libraries
COPY requirements.r .
RUN Rscript requirements.r


COPY results_processor.R .
COPY results_collector.py .
COPY reporter.py .
COPY data_manager.py .

COPY start.sh .


ENTRYPOINT ["/app/start.sh"]