GOVERSION=$(shell go version)
GOOS=$(shell go env GOOS)
GOARCH=$(shell go env GOARCH)
VERSION=$(patsubst "%",%,$(lastword $(shell grep 'const Version' cmd/version.go)))
ARTIFACTS_DIR=$(CURDIR)/artifacts/$(VERSION)
RELEASE_DIR=$(CURDIR)/release/$(VERSION)
SRC_FILES = $(shell find . -name *.go -and -type f)
GITHUB_USERNAME=shogo82148

.PHONY: all test clean

all: build-windows-amd64 build-linux-amd64 build-darwin-amd64

#### dependency management

installdeps:
	go get github.com/golang/dep/cmd/dep
	dep ensure

##### build settings

.PHONY: build build-windows-amd64 build-linux-amd64 build-darwin-amd64

$(ARTIFACTS_DIR)/s3cli-mini_$(GOOS)_$(GOARCH):
	@mkdir -p $@

$(ARTIFACTS_DIR)/s3cli-mini_$(GOOS)_$(GOARCH)/s3cli-mini$(SUFFIX): $(ARTIFACTS_DIR)/s3cli-mini_$(GOOS)_$(GOARCH) $(SRC_FILES)
	@echo " * Building binary for $(GOOS)/$(GOARCH)..."
	@CGO_ENABLED=0 go build -o $@ main.go

build: $(ARTIFACTS_DIR)/s3cli-mini_$(GOOS)_$(GOARCH)/s3cli-mini$(SUFFIX)

build-windows-amd64:
	@$(MAKE) build GOOS=windows GOARCH=amd64 SUFFIX=.exe

build-linux-amd64:
	@$(MAKE) build GOOS=linux GOARCH=amd64

build-darwin-amd64:
	@$(MAKE) build GOOS=darwin GOARCH=amd64

##### release settings

.PHONY: release-windows-amd64 release-linux-amd64 release-darwin-amd64
.PHONY: release-targz release-zip release-files release-upload

$(RELEASE_DIR)/s3cli-mini_$(GOOS)_$(GOARCH):
	@mkdir -p $@

release-windows-amd64:
	@$(MAKE) release-zip GOOS=windows GOARCH=amd64 SUFFIX=.exe

release-linux-amd64:
	@$(MAKE) release-targz GOOS=linux GOARCH=amd64

release-darwin-amd64:
	@$(MAKE) release-zip GOOS=darwin GOARCH=amd64

release-targz: build $(RELEASE_DIR)/s3cli-mini_$(GOOS)_$(GOARCH)
	@echo " * Creating tar.gz for $(GOOS)/$(GOARCH)"
	tar -czf $(RELEASE_DIR)/s3cli-mini_$(GOOS)_$(GOARCH).tar.gz -C $(ARTIFACTS_DIR) s3cli-mini_$(GOOS)_$(GOARCH)

release-zip: build $(RELEASE_DIR)/s3cli-mini_$(GOOS)_$(GOARCH)
	@echo " * Creating zip for $(GOOS)/$(GOARCH)"
	cd $(ARTIFACTS_DIR) && zip -9 $(RELEASE_DIR)/s3cli-mini_$(GOOS)_$(GOARCH).zip s3cli-mini_$(GOOS)_$(GOARCH)/*

release-files: release-windows-amd64 release-linux-amd64 release-darwin-amd64

release-upload: release-files
	ghr -u $(GITHUB_USERNAME) --draft --replace v$(VERSION) $(RELEASE_DIR)

test:
	go test -v -race ./...
	go vet ./...

clean:
	-rm -rf vendor
