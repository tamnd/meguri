BIN     := meguri
PKG     := ./cmd/meguri
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X github.com/tamnd/meguri.Version=$(VERSION) \
	-X github.com/tamnd/meguri.Commit=$(COMMIT) \
	-X github.com/tamnd/meguri.Date=$(DATE)

export CGO_ENABLED := 0

.PHONY: build install test test-short bench scale-smoke scale-full vet fmt tidy clean run

build:
	go build -ldflags "$(LDFLAGS)" -o bin/$(BIN) $(PKG)

install:
	go install -ldflags "$(LDFLAGS)" $(PKG)

# Full suite with the race detector on.
test:
	go test -race ./...

# Quick loop without the race detector.
test-short:
	go test -short ./...

bench:
	go test -bench=. -benchmem -run='^$$' ./...

# Timed scale smoke run on the 10k profile (fast, catches breakage, not a number of
# record). Build the corpus slices first with scripts/build-profiles.sh.
scale-smoke: build
	bin/$(BIN) scale -i corpus/profiles/scale-10k.jsonl --profile 10k --commit $(COMMIT)

# Timed scale run on the full pinned corpus. Pass --box on a box of record for a
# number of record; without it the run is a smoke run.
scale-full: build
	bin/$(BIN) scale -i corpus/urls.jsonl --profile 142k --commit $(COMMIT)

vet:
	go vet ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

clean:
	rm -rf bin dist

run: build
	./bin/$(BIN)
