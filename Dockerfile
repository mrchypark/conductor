FROM alpine:3.22
ARG TARGETARCH
COPY target/conductor-linux-${TARGETARCH} /usr/local/bin/conductor
ENTRYPOINT ["conductor"]
