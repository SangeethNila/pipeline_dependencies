.PHONY: up up-browser down logs

up:
	docker compose up

up-browser:
	docker compose up -d
	@echo "Waiting for Neo4j to start..."
	@sleep 10
	@until curl -s http://localhost:7474 > /dev/null; do \
		echo "Waiting for Neo4j..."; \
		sleep 2; \
	done
	@echo "Opening Neo4j Browser..."
	open "http://localhost:7474/browser/?cmd=connect"
	docker compose logs -f

down:
	docker compose down

logs:
	docker compose logs -f
