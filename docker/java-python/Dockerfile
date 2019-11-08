FROM openjdk:8-jre-slim

RUN apt-get update && \
    apt-get install -y  --no-install-recommends \
                    wget \
                    xz-utils \
                    build-essential \
                    libsqlite3-dev \
                    libreadline-dev \
                    libssl-dev \
                    zlib1g-dev \
                    software-properties-common \
                    openssl && \
    wget --no-check-certificate https://www.python.org/ftp/python/3.6.5/Python-3.6.5.tar.xz && \
    tar -xf Python-3.6.5.tar.xz && \
    cd Python-3.6.5 && ./configure && make && make install && \
    apt-get -y install --no-install-recommends python3-pip && \
    pip3 install --upgrade pip && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /Python-3.6.5.tar.xz /Python-3.6.5
