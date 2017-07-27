FROM alpine:3.5

RUN apk add --no-cache ca-certificates curl
RUN curl -L https://storage.googleapis.com/kubernetes-release/release/v1.6.4/bin/linux/amd64/kubectl -o /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl
ADD bin/e2e.test /usr/local/bin/e2e.test
ADD tidb-operator.yaml.tmpl /tmp/tidb-operator.yaml.tmpl
ADD data /tmp/data

CMD ["/usr/local/bin/e2e.test", "-ginkgo.v"]
