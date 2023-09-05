FROM debian:bookworm
ARG LK_VERSION
ARG CKPY_VERSION
RUN test -n "${LK_VERSION}" || (echo "LK_VERSION env variable required" && exit 1)
RUN test -n "${CKPY_VERSION}" || (echo "CKPY_VERSION env variable required" && exit 1)
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y sudo
RUN mkdir -p /soaktests
COPY bootstrap.sh /soaktests
WORKDIR /soaktests
RUN /soaktests/bootstrap.sh ${CKPY_VERSION} ${LK_VERSION}
ENTRYPOINT [ "/soaktests/confluent-kafka-python/tests/soak/run.sh" ]
