# Omni-Joiner make targets
# Requires: Go 1.22+

.PHONY: build test lint lint-fix lint-install

build:
	go build -o bin/processor ./cmd/processor

test:
	go test ./...

# Run linter (uses go run so no separate install needed; first run may download golangci-lint)
lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run

# Fix goimports formatting then run linter (fixes import order and file formatting)
lint-fix:
	go run golang.org/x/tools/cmd/goimports@latest -w -local github.com/confluo/omni-joiner .
	$(MAKE) lint

# Optional: install golangci-lint to $(go env GOPATH)/bin for faster repeated runs
lint-install:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
