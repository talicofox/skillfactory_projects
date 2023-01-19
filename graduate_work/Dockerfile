FROM ubuntu:latest

RUN apt-get update \
    && apt-get install -qyy -o APT::Install-Recommends=false -o APT::Install-Suggests=false \
    file \
    gcc \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools \
    python3-venv \
    python3-wheel \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --cache-dir=/tmp/pipcache --upgrade pip && rm -rf /tmp/pipcache
RUN pip install --cache-dir=/tmp/pipcache poetry && rm -rf /tmp/pipcache


FROM python:latest

WORKDIR /src/app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY /src/ .

CMD ["python", "./main.py"]