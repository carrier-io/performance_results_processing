FROM r-base:4.3.1
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    git libcurl4-openssl-dev libssl-dev libxml2-dev build-essential libpq-dev \
    python3-setuptools python3.13 python3.13-venv python3.13-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/app/venv
RUN python3.13 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python -m pip install --upgrade pip

RUN pip install "setuptools<82"
# installing r libraries
COPY requirements.r .
RUN Rscript requirements.r

## installing python libraries
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY results_processor.R .
COPY reporter.py .
COPY data_manager.py .
COPY results_collector.py .
COPY models.py .
COPY utils.py .

COPY start.sh .


ENTRYPOINT ["/app/start.sh"]