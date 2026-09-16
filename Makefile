# Makefile — bioclimamx/conagua-etl
#
# Standard entry points. `make check` is the pre-commit gate.

BIN := build/conagua-etl
PKG := ./...

.PHONY: build test check fmt vet lint tidy clean

# Release binaries are CGo-free: the SQLite
# driver and the Parquet library are pure Go so the citable binary
# cross-compiles; `build` and `check` hold the module to that posture.
build:
	CGO_ENABLED=0 go build -o $(BIN) ./cmd/conagua-etl

test:
	go test $(PKG)

# Pre-commit gate: formatting + vet + lint + tests + the CGo-free build
# must all pass.
check:
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	go vet $(PKG)
	golangci-lint run
	go test $(PKG)
	CGO_ENABLED=0 go build $(PKG)

fmt:
	gofmt -w .

vet:
	go vet $(PKG)

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -rf build/
