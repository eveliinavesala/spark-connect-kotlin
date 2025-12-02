# Kotlin Notebook Integration

This directory contains example notebooks that demonstrate how to use our Kotlin-Spark-API extensions in an interactive environment.

## In-IDE Workflow

This project is configured to provide a seamless, in-IDE Kotlin Notebook experience. The `org.jetbrains.kotlin.notebook` Gradle plugin allows the IDE to automatically use the project's build file as the single source of truth for all dependencies.

### How to Run the Notebooks

1.  **Build the Project:**
    Ensure the latest version of our library code is compiled. This makes our `pragmatic` package available to the notebook.
    ```sh
    ./gradlew build
    ```

2.  **Open the Notebook in your IDE:**
    Open the `notebooks/GettingStarted.ipynb` file directly in a compatible IDE (e.g., IntelliJ IDEA with the Kotlin Notebook plugin).

3.  **Run the Cells:**
    You can now execute the cells in the notebook. The IDE will automatically use the project's classpath. All dependencies, including the Spark Connect client, `kotlin-reflect`, and our own `pragmatic` library code, will be available without any `%use` or `@file:DependsOn` commands in the notebook itself.
