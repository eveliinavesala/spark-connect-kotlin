# --- Variables ---
IMAGE_NAME = spark-server
CONTAINER_NAME = spark-container

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

# --- Utility Commands ---

.PHONY: build run stop clean app all rebuild refresh

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