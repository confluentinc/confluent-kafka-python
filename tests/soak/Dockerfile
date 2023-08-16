FROM debian:bookworm
ARG LK_VERSION=v2.2.0
ARG CKPY_VERSION=v2.2.0
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y sudo
RUN mkdir -p /soaktests
COPY bootstrap.sh /soaktests
WORKDIR /soaktests
RUN /soaktests/bootstrap.sh ${CKPY_VERSION} ${LK_VERSION}
ENTRYPOINT [ "/soaktests/confluent-kafka-python/tests/soak/run.sh" ]
