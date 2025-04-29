.DEFAULT_GOAL := help

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test-setup: ## Setup test environment
	docker compose up -d

test-teardown: ## Teardown test environment
	docker compose down

test: ## Run tests
	go test -race -tags=integration ./...
