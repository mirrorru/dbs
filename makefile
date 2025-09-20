.DEFAULT_GOAL := help

## help: Print available commands
.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Available targets:"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""

LOCAL_BIN := $(CURDIR)/bin

GOLANGCI_VERSION := 2.3.0

.PHONY: test
test: ## Starts unit-tests
	@echo "Start unit-tests..."
	go test ./...

.PHONY: install-bin
install-bin: .install-golangci ## Install binaries

.PHONY: .install-golangci
.install-golangci: ## Install golangci linter
	mkdir -p bin
	#GOBIN=$(LOCAL_BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(LOCAL_BIN) v2.3.1

.PHONY: lint
lint: ## code checks with golangci
	clear
	$(LOCAL_BIN)/golangci-lint run \
		--config=.golangci.yaml \
		--new-from-rev=origin/main \
		--max-issues-per-linter=100 \
		--max-same-issues=50 \
		./...

