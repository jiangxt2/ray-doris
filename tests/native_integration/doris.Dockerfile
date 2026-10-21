ARG DORIS_BASE_IMAGE
FROM ${DORIS_BASE_IMAGE}
ARG DORIS_ROLE
COPY tests/integration/docker/${DORIS_ROLE}.conf /opt/apache-doris/${DORIS_ROLE}/conf/${DORIS_ROLE}.conf
