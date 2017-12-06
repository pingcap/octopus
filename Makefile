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

build: tidb-benchbot stability-tester ycsb

tidb-benchbot:
	$(GOBUILD) -o bin/tidb-benchbot benchbot/*.go

tidb-stability:
	$(GOBUILD) -o bin/tidb-stability-tester stability-tester/*.go

tidb-ycsb:
	$(GOBUILD) -o bin/tidb-ycsb ycsb/*.go

bank:
	$(GOBUILD) -o bin/bank stability-tester/bank/*.go

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
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
ifdef PKG
	glide get --strip-vendor --skip-test ${PKG}
else
	glide update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests

clean:
	@rm -rf bin/tidb-benchbot

.PHONY: update clean
