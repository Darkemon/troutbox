.DEFAULT_GOAL := help

GOLANGCI_LINT_VERSION := v2.1.2
MOCKERY_VERSION := v3.2.2

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run unit tests
	go test -race ./...

test-integration: ## Run integration tests
	( cd tests/integration && make test-setup test test-teardown )

lint: ## Run linter
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run

gen-mocks: ## Generate mocks
	go run github.com/vektra/mockery/v3@$(MOCKERY_VERSION)
