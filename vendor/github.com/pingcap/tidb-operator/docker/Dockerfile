FROM alpine:3.5

RUN apk add --no-cache ca-certificates

ADD bin/tidb-operator /usr/local/bin/tidb-operator
ADD bin/tidb-volume-manager /usr/local/bin/tidb-volume-manager
ADD bin/tidb-scheduler /usr/local/bin/tidb-scheduler

CMD ["/bin/sh", "-c", "/usr/local/bin/tidb-operator"]
