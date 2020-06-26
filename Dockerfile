FROM golang:alpine as build

RUN mkdir -p /build
COPY ./ /build/
ENV CGO_ENABLED=0

RUN cd /build/ && go build ./... && go install ./... 
RUN cd /go/bin && for p in *; do mv "$p" "ega-$p"; done
                              
RUN addgroup -g 1000 lega && \     
    adduser -D -u 1000 -G lega lega

FROM scratch

ARG BUILD_DATE
ARG SOURCE_COMMIT

LABEL maintainer "NeIC System Developers"
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.build-date=$BUILD_DATE
LABEL org.label-schema.vcs-url="https://github.com/neicnordic/sda-pipeline"
LABEL org.label-schema.vcs-ref=$SOURCE_COMMIT

COPY --from=build /go/bin/ /
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group

USER 1000

