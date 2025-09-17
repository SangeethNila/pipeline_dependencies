FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    golang \
    git \
    vim \
    && rm -rf /var/lib/apt/lists/*
COPY . /app
WORKDIR /app
RUN pip3 install -r requirements.txt --break-system-packages