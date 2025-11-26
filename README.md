# Spark Connect Kotlin Example

This project demonstrates a basic Kotlin application connecting to a Spark 4.0 cluster using Spark Connect.

## Prerequisites

*   [Docker](https://www.docker.com/get-started) must be installed and running.
*   A Java 17 JDK.
*   An IDE that supports Gradle projects (e.g., IntelliJ IDEA).

## Setup and Execution

There are two main components: the Spark Connect server (running in Docker) and the Kotlin client application.

### 1. Run the Spark Connect Server

The Spark Connect server is packaged in a Docker container for easy setup.

**Build the Docker image:**
Open a terminal in the project root and run the following command. You only need to do this once.
```sh
make build
```

**Run the Docker container:**
After the image is built, start the Spark Connect server with this command:
```sh
make run
```
This will start a container with the Spark server and expose the Spark Connect port `15002`. The server may take 15-20 seconds to initialize fully. You can check the container logs to see when the service is ready.

### 2. Run the Kotlin Application

Once the Docker container is running, you can execute the client application.

1.  Open the project in your IDE and let Gradle sync the dependencies.
2.  Navigate to `src/main/kotlin/app/kotlin_spark/Main.kt`.
3.  Run the `main` function.

You may run your application also with make.
```sh
make app
```

If successful, you will see a DataFrame printed to the console, confirming that the Kotlin application has successfully connected to the Spark server and executed a query.

## Running Tests

This project uses [Testcontainers](https://www.testcontainers.org/) to automatically manage a Spark container for running unit and integration tests.

To run the tests, execute the following command in your terminal:

```sh
make build && ./gradlew test
```

This will download the necessary Docker image, start the Spark container, run the tests, and then shut down the container. You can also run the tests directly from your IDE.
