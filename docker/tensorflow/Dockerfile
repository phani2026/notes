FROM openjdk:8-jre-alpine3.7

ENV LOCAL_RESOURCES 2048,.5,1.0

RUN apk add --no-cache python3 python3-tkinter py3-numpy py3-numpy-f2py freetype libpng libjpeg-turbo imagemagick graphviz git
RUN apk add --no-cache --virtual=.build-deps \
        bash \
        cmake \
        curl \
        freetype-dev \
        g++ \
        libjpeg-turbo-dev \
        libpng-dev \
        linux-headers \
        make \
        musl-dev \
        openblas-dev \
        patch \
        perl \
        python3-dev \
        py-numpy-dev \
        rsync \
        sed \
        swig \
        zip \
    && : prepare for building TensorFlow \
    && : install wheel python module \
    && cd /tmp \
    && pip3 install --no-cache-dir wheel \
    && : \
    && : add python symlink to avoid python detection error \
    && $(cd /usr/bin && ln -s python3 python) \
    && : install Bazel to build TensorFlow \
    && pip3 install --no-cache-dir pandas scipy jupyter \
    && pip3 install --no-cache-dir scikit-learn matplotlib Pillow \
    && pip3 install --no-cache-dir google-api-python-client \
    && : \
    && : clean up unneeded packages and files \
    && apk del .build-deps \
    && rm -f /usr/bin/bazel \
    && rm -rf /tmp/* /root/.cache

