# --- Variables ---
IMAGE_NAME = spark-server
IMAGE_NAME_UNITY = spark-unity
CONTAINER_NAME = spark-container
UC_CONTAINER_NAME = unity-catalog
PG_CONTAINER_NAME = uc-postgres
COMPOSE_FILE = docker-compose.yml

# --- Docker Commands ---

# Build the Docker image for the Spark Connect server
build:
	docker build -t $(IMAGE_NAME) .

# Run the Spark Connect server in a detached container
run:
	docker run -d --name $(CONTAINER_NAME) -p 15002:15002 $(IMAGE_NAME)

# Stop the Spark Connect server container
stop:
	docker stop $(CONTAINER_NAME)

# Remove the stopped Spark Connect server container
clean:
	docker rm $(CONTAINER_NAME)

# --- Application Commands ---

# Run the Kotlin client application
app:
	./gradlew run

# Rebuild the application without using the Gradle task cache
rebuild:
	./gradlew build --no-cache

# Force Gradle to refresh and re-download all dependencies
refresh:
	./gradlew build --refresh-dependencies

# --- Unity Catalog Commands ---

# Generate docker/unity-catalog/hibernate.properties from template + .env variables
generate-config:
	@set -a && . ./.env && set +a && \
	envsubst < docker/unity-catalog/hibernate.properties.template \
	          > docker/unity-catalog/hibernate.properties
	@echo "Generated docker/unity-catalog/hibernate.properties"

# Build all Unity Catalog images
uc-build:
	docker compose -f $(COMPOSE_FILE) build

# Start Unity Catalog stack (postgres + unity + spark)
uc-up:
	docker compose -f $(COMPOSE_FILE) up -d

# Stop Unity Catalog stack
uc-down:
	docker compose -f $(COMPOSE_FILE) down

# Stop and remove volumes (cleans all data)
uc-clean:
	docker compose -f $(COMPOSE_FILE) down -v

# Show logs from all services
uc-logs:
	docker compose -f $(COMPOSE_FILE) logs -f

# Show Unity Catalog logs only
uc-logs-unity:
	docker compose -f $(COMPOSE_FILE) logs -f unity

# Show Spark logs only
uc-logs-spark:
	docker compose -f $(COMPOSE_FILE) logs -f spark

# Show PostgreSQL logs only
uc-logs-postgres:
	docker compose -f $(COMPOSE_FILE) logs -f postgres

# Check health of all services
uc-status:
	docker compose -f $(COMPOSE_FILE) ps

# Restart Unity Catalog stack
uc-restart:
	docker compose -f $(COMPOSE_FILE) restart

# Build and start Unity Catalog (full workflow)
uc-start: generate-config uc-build uc-up
	@echo "Waiting for services to be healthy..."
	sleep 10
	$(MAKE) uc-status

# --- Test commands ---

test:
	make build && ./gradlew test

# Test with Unity Catalog stack running
uc-test:
	make uc-build && ./gradlew test

stacktrace:
	make build && ./gradlew test --stacktrace --info

# Test with Unity Catalog and detailed output
uc-stacktrace:
	make uc-build && ./gradlew test --stacktrace --info

# --- Utility Commands ---

.PHONY: build run stop clean app all rebuild refresh test stacktrace \
        generate-config \
        uc-build uc-up uc-down uc-clean uc-logs uc-logs-unity uc-logs-spark uc-logs-postgres \
        uc-status uc-restart uc-start uc-test uc-stacktrace

# Full workflow: build, run server, run app, then stop and clean up. Extra sleep time is to avoid race condition.
all:
	$(MAKE) build
	sleep 10
	$(MAKE) run
	@echo "Server container started. Waiting for it to initialize."
	sleep 15
	$(MAKE) app
	@echo "Application finished. Cleaning up."
	$(MAKE) stop
	$(MAKE) clean
	@echo "--- Workflow complete ---"