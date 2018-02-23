GO=GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 go
GOTEST=GO15VENDOREXPERIMENT="1" CGO_ENABLED=1 go test # go race detector requires cgo

PACKAGES := $$(go list ./...| grep -vE 'vendor')

GOFILTER := grep -vE 'vendor|render.Delims|bindata_assetfs|testutil'
GOCHECKER := $(GOFILTER) | awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "github.com/pingcap/octopus/pkg/util.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/octopus/pkg/util.GitHash=$(shell git rev-parse HEAD)"

GOBUILD=$(GO) build -ldflags '$(LDFLAGS)'

default: build

all: build check test

build: tidb-benchbot ycsb

tidb-benchbot:
	# go-sqlite3 require CGO, see https://github.com/mattn/go-sqlite3/issues/327
	CGO_ENABLED=1 go build -ldflags '$(LDFLAGS)' -o bin/tidb-benchbot benchbot/*.go

tidb-ycsb:
	$(GOBUILD) -o bin/tidb-ycsb ycsb/*.go

bank:
	$(GOBUILD) -o bin/bank stability-tester/bank/*.go

cross-table-bank:
	$(GOBUILD) -o bin/cross-table-bank stability-tester/cross-table-bank/*.go

stmt_retry:
	$(GOBUILD) -o bin/stmt_retry stability-tester/stmt_retry/*.go

sysbench-test:
	$(GOBUILD) -o bin/sysbench-test stability-tester/sysbench/*.go

bank2:
	$(GOBUILD) -o bin/bank2 stability-tester/bank2/*.go

block_writer:
	$(GOBUILD) -o bin/block_writer stability-tester/block_writer/*.go

crud:
	$(GOBUILD) -o bin/crud stability-tester/crud/*.go

ledger:
	$(GOBUILD) -o bin/ledger stability-tester/ledger/*.go

log:
	$(GOBUILD) -o bin/log stability-tester/log/*.go

small_writer:
	$(GOBUILD) -o bin/small_writer stability-tester/small_writer/*.go

mvcc_bank:
	$(GOBUILD) -o bin/mvcc_bank stability-tester/mvcc_bank/*.go

sqllogictest:
	$(GOBUILD) -o bin/sqllogictest stability-tester/sqllogictest/*.go

shuffle:
	$(GOBUILD) -o bin/shuffle stability-tester/shuffle/*.go

ddl:
	$(GOBUILD) -o bin/ddl stability-tester/ddl/*.go

on_dup:
	$(GOBUILD) -o bin/on_dup stability-tester/on_dup/*.go

test:
	$(GOTEST) --race $(PACKAGES)

check:
	go get github.com/golang/lint/golint

	@echo "vet"
	@ go tool vet . 2>&1 | $(GOCHECKER)
	@ go tool vet --shadow . 2>&1 | $(GOCHECKER)
	@echo "golint"
	@ golint ./... 2>&1 | $(GOCHECKER)
	@echo "gofmt"
	@ gofmt -s -l . 2>&1 | $(GOCHECKER)

update:
	which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
ifdef PKG
	dep ensure -add ${PKG}
else
	dep ensure -update
endif
	@echo "removing test files"
	dep prune
	bash ./clean_vendor.sh

clean:
	@rm -rf bin/tidb-benchbot

.PHONY: update clean
