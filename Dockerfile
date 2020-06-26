FROM golang:alpine as builder

ENV GOPATH=$PWD
ENV CGO_ENABLED=0

COPY . .

RUN for p in cmd/*; do go build -o "${p/cmd\//sda-}" "./$p"; done
RUN echo "nobody:x:65534:65534:nobody:/:/sbin/nologin" > passwd

FROM scratch

ARG BUILD_DATE
ARG SOURCE_COMMIT

LABEL maintainer "NeIC System Developers"
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.build-date=$BUILD_DATE
LABEL org.label-schema.vcs-url="https://github.com/neicnordic/sda-pipeline"
LABEL org.label-schema.vcs-ref=$SOURCE_COMMIT

COPY --from=builder /go/passwd /etc/passwd
COPY --from=builder /go/sda-* /

USER 65534
