FROM jpy:tf


WORKDIR /

COPY rztdl-0.2.1-py3-none-any.whl/ /data/

ENV PYTHONPATH /usr/local/lib/python3.6/site-packages
ENV BBSDK_CONFIG /data/config.json
ENV ENGINE_PORT 80
ENV ENGINE_HOST engine-core

ENV LOCAL_RESOURCES 2048,.5,1.0

ENV BAZEL_VERSION 0.15.0
ENV TENSORFLOW_VERSION 1.8.0

RUN apk add --no-cache --virtual=.build-deps curl

RUN : prepare for building TensorFlow \
    && : \
    && : install Bazel to build TensorFlow \
    && curl -SLO https://github.com/bazelbuild/bazel/releases/download/${BAZEL_VERSION}/bazel-${BAZEL_VERSION}-dist.zip \
    && mkdir bazel-${BAZEL_VERSION} \
    && unzip -qd bazel-${BAZEL_VERSION} bazel-${BAZEL_VERSION}-dist.zip \
    && cd bazel-${BAZEL_VERSION} \
    && : add -fpermissive compiler option to avoid compilation failure \
    && sed -i -e '/"-std=c++0x"/{h;s//"-fpermissive"/;x;G}' tools/cpp/cc_configure.bzl \
    && : add '#include <sys/stat.h>' to avoid mode_t type error \
    && sed -i -e '/#endif  \/\/ COMPILER_MSVC/{h;s//#else/;G;s//#include <sys\/stat.h>/;G;}' third_party/ijar/common.h \
    && sh compile.sh \
    && cp -p output/bazel /usr/bin/ \
    && : \
    && : build TensorFlow pip package \
    && cd /tmp \
    && curl -SL https://github.com/tensorflow/tensorflow/archive/v${TENSORFLOW_VERSION}.tar.gz \
        | tar xzf - \
    && cd tensorflow-${TENSORFLOW_VERSION} \
    && : musl-libc does not have "secure_getenv" function \
    && sed -i -e '/JEMALLOC_HAVE_SECURE_GETENV/d' third_party/jemalloc.BUILD \
    && PYTHON_BIN_PATH=/usr/bin/python \
        PYTHON_LIB_PATH=/usr/lib/python3.6/site-packages \
        CC_OPT_FLAGS="-march=native" \
        TF_NEED_JEMALLOC=1 \
        TF_NEED_GCP=0 \
        TF_NEED_HDFS=0 \
        TF_NEED_S3=0 \
        TF_ENABLE_XLA=0 \
        TF_NEED_GDR=0 \
        TF_NEED_VERBS=0 \
        TF_NEED_OPENCL=0 \
        TF_NEED_CUDA=0 \
        TF_NEED_MPI=0 \
        sh configure \
    && bazel build -c opt --local_resources ${LOCAL_RESOURCES} //tensorflow/tools/pip_package:build_pip_package \
    && ./bazel-bin/tensorflow/tools/pip_package/build_pip_package /tmp/tensorflow_pkg \
    && : \
    && : install python modules including TensorFlow \
    && cd \
    && : \
    && : clean up unneeded packages and files \
    && apk del .build-deps \
    && rm -f /usr/bin/bazel \
    && rm -rf /tmp/* /root/.cache



EXPOSE 9094

ENTRYPOINT ["/bin/ash"]