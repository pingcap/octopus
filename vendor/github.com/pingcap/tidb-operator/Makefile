### Makefile for tidb-opeator

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
    $(error Please set the environment variable GOPATH before running `make`)
endif

path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

VERSION   := $(if $(VERSION),$(VERSION),latest)
GO        := GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go
GOTEST    := GO15VENDOREXPERIMENT="1" CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go test
FILES     := $$(find . -name '*.go' -type f | grep -vE 'vendor' | grep -vE 'glide')
E2E_FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor' | grep -vE 'glide' | grep 'e2e')
GOFILTER  := grep -vE 'vendor|render.Delims|bindata_assetfs|testutil|election'
GOCHECKER := $(GOFILTER) | awk '{ print } END { if (NR > 0) { exit 1 } }'

PACKAGES := $$(go list ./...| grep -vE 'vendor')

LDFLAGS += -X "github.com/pingcap/tidb-operator/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-operator/version.GitSHA=$(shell git rev-parse HEAD)"

E2E_VERSION     := $(shell git rev-parse --short HEAD)-$(shell date -u '+%Y-%m-%d_%I-%M-%S')
DOCKER_REGISTRY := $(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY),127.0.0.1:5000)

default: build

all: dev install

dev: check test build

check:
	bash gitcookie.sh
	go get github.com/golang/lint/golint
	@echo "vet"
	@ go tool vet $(FILES) 2>&1 | $(GOCHECKER)
	@echo "vet --shadow"
	@ go tool vet --shadow $(FILES) 2>&1 | $(GOCHECKER)
	@echo "golint"
	@ golint pkg/... cmd/... 2>&1 | $(GOCHECKER)
	@echo "golint for e2e test"
	@ golint test/... 2>&1 | grep -vE 'should not use dot imports' | $(GOCHECKER)
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | $(GOCHECKER)

test:
	$(GOTEST) --race $(PACKAGES)
	$(GOTEST) --cover $(PACKAGES)

operator:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/tidb-operator cmd/operator/main.go
scheduler:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/tidb-scheduler cmd/scheduler/main.go

volume-manager:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/tidb-volume-manager cmd/volume-manager/main.go

build: operator scheduler volume-manager

docker: build
	mkdir -p docker/bin && cp bin/tidb-* docker/bin/
	cd docker && docker build --tag "$(DOCKER_REGISTRY)/pingcap/tidb-operator:$(VERSION)" .
	docker push "$(DOCKER_REGISTRY)/pingcap/tidb-operator:$(VERSION)"

e2e:
	sed 's/image: pingcap\/tidb-operator:v.*/image: {{ .Image }}/g' example/tidb-operator.yaml > \
		test/e2e/docker/tidb-operator.yaml.tmpl
	mkdir -p test/e2e/docker/bin && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOPATH)/bin/ginkgo build test/e2e && \
		mv test/e2e/e2e.test test/e2e/docker/bin/
	cd test/e2e/docker && docker build --tag "$(DOCKER_REGISTRY)/pingcap/tidb-operator-e2e:$(E2E_VERSION)" .
	docker push "$(DOCKER_REGISTRY)/pingcap/tidb-operator-e2e:$(E2E_VERSION)"

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
	$(GO) clean -i ./...
	rm -rf bin/tidb-operator

.PHONY: update clean
