# Kotlin Notebook Integration (Local Development Workflow)

This document describes a pragmatic workflow for using Kotlin Notebooks for **local, interactive development** of this library.

This approach uses a manual IDE run configuration to provide the notebook with the project's full classpath, including all dependencies and our compiled `pragmatic` code. This is a powerful method for rapid prototyping and debugging without needing to publish the library locally.

### How to Set Up and Run

1.  **Start the Spark Server:**
    Ensure the Spark Connect server is running. You can start it using our Docker setup:
    ```sh
    make docker-build
    # (Then run the container, exposing port 15002)
    ```

2.  **Build the Project:**
    Make sure the latest version of our library code is compiled. This is essential for the IDE to find our `pragmatic` package.
    ```sh
    ./gradlew build
    ```

3.  **Create an IDE Run Configuration (The Core of the Setup):**
    We will create a run configuration that uses the `main` function of our application to establish the correct classpath for the notebook.

    *   In your IDE (e.g., IntelliJ IDEA), go to "Run/Debug Configurations".
    *   Click the **+** button and select **Kotlin**.
    *   Name the configuration something descriptive, like `Notebook Environment`.
    *   For the **Main class**, select `app.kotlin_spark.MainKt`. This is the key step that provides the notebook with all the necessary project dependencies.
    *   In the **VM options** field, add the following arguments to ensure compatibility with Spark's reflection needs on modern JVMs:
        ```
        --add-opens=java.base/java.nio=ALL-UNNAMED
        --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
        ```

4.  **Run the Configuration:**
    Select the newly created `Notebook Environment` configuration and run it. This will start a process with the correct classpath.

5.  **Interact with the Notebook:**
    While the `Notebook Environment` process is running, open the `notebooks/GettingStartedKotlin.ipynb` file in your IDE. You can now execute the cells one by one. The notebook kernel will attach to the running process and will have full access to Spark, our `pragmatic` library, and all other dependencies.
