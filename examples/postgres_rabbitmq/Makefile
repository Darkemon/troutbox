.DEFAULT_GOAL := help

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

infra-up: ## Start the infrastructure
	docker compose up -d
	@echo ""
	@echo "To see the RabbitMQ management interface go to:"
	@echo "http://guest:guest@localhost:15672"
	@echo ""

infra-down: ## Stop the infrastructure
	docker compose down
