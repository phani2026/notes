FROM ubuntu:16.04


RUN apt-get update && \
    apt-get install -y  --no-install-recommends \
                    wget \
                    xz-utils \
                    ssh \
                    build-essential \
                    libsqlite3-dev \
                    libreadline-dev \
                    libssl-dev \
                    zlib1g-dev \
                    software-properties-common


RUN mkdir -p /root/.ssh
COPY creds .ssh
COPY creds /root/.ssh

RUN chmod 400 ~/.ssh/id_rsa