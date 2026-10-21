ARG HADOOP_IMAGE=apache/gravitino-ci:hive-0.1.20
FROM ${HADOOP_IMAGE} AS hadoop
FROM rayproject/ray:2.59.0-py312
USER root
COPY --from=hadoop /opt/hadoop-3.3.0 /opt/hadoop
COPY --from=hadoop /usr/lib/jvm/java-8-openjdk-arm64 /opt/java
ENV JAVA_HOME=/opt/java HADOOP_HOME=/opt/hadoop ARROW_LIBHDFS_DIR=/opt/native-hdfs HADOOP_USER_NAME=root RAY_ENABLE_UV_RUN_RUNTIME_ENV=0
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev
COPY --from=hadoop-native-source . /opt/hdfs-source
RUN mkdir -p /opt/native-hdfs && \
    printf '#define HAVE_BETTER_TLS 1\n' > /opt/hdfs-source/config.h && \
    gcc -shared -fPIC -DLIBHDFS_DLL_EXPORT \
    -I/opt/java/include -I/opt/java/include/linux \
    -I/opt/hdfs-source -I/opt/hdfs-source/include -I/opt/hdfs-source/os/posix \
    /opt/hdfs-source/hdfs.c /opt/hdfs-source/jni_helper.c /opt/hdfs-source/exception.c \
    /opt/hdfs-source/jclasses.c /opt/hdfs-source/os/posix/mutexes.c \
    /opt/hdfs-source/os/posix/thread_local_storage.c \
    -L/opt/java/jre/lib/aarch64/server -Wl,-rpath,/opt/java/jre/lib/aarch64/server \
    -ljvm -ldl -lpthread -o /opt/native-hdfs/libhdfs.so && \
    python -c "import ctypes; ctypes.CDLL('/opt/native-hdfs/libhdfs.so')"
WORKDIR /workspace
COPY pyproject.toml ./
RUN python -c "import tomllib; p=tomllib.load(open('pyproject.toml','rb')); print('\n'.join(p['project']['dependencies'] + sum((p['project']['optional-dependencies'][g] for g in ['native','test','flight']), []) + p['build-system']['requires']))" > /tmp/native-requirements.txt && \
    pip install -r /tmp/native-requirements.txt 'ray[data]==2.59.0' 'pyarrow==26.0.0'
COPY README.md LICENSE NOTICE ./
COPY src ./src
RUN pip install --no-deps --no-build-isolation .
COPY tests ./tests
ENV LD_PRELOAD=/opt/java/jre/lib/aarch64/libjsig.so
RUN test -f "$LD_PRELOAD"
ENTRYPOINT ["bash", "/workspace/tests/native_integration/ray-entrypoint.sh"]
