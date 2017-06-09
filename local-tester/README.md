# Local tester

Port from [etcd local-tester](https://github.com/coreos/etcd/tree/master/tools/local-tester).

## Building

### goreman

```bash
go get github.com/mattn/goreman
```

### bridge

```go
go build bridge/bridge.go -o ./bin/bridge
```

### PD/TiDB/TiKV

See https://github.com/pingcap/docs-cn/blob/master/op-guide/binary-deployment.md#linux-centos-7-ubuntu-1404

Move all binaries to `bin` directory.

## Running

```bash
goreman start

# Run cases
```