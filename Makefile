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
