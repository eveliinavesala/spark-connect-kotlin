# Aims and Strategy for Modernizing Kotlin Support in Apache Spark with Spark Connect

---

## 1. Executive Summary

This document outlines the research aims and development strategy for a thesis project focused on enabling idiomatic Kotlin development for Apache Spark 4.0's new client-server architecture, Spark Connect. The current Spark API, while functional, is Java-centric and does not fully support modern Kotlin features, leading to a suboptimal developer experience and potential runtime errors. The community-driven `kotlin-spark-api` library provides an excellent idiomatic wrapper but was designed for older, single-process Spark versions and is incompatible with Spark Connect.

This project's primary aim is to bridge this gap by adapting the `kotlin-spark-api` to be fully functional and performant with Spark Connect. The strategy involves establishing a baseline test suite using idiomatic Kotlin, performing a gap analysis of the existing library against the new architecture, and re-implementing the library's core components to address serialization and remote execution challenges. The final deliverable will be an updated, open-source library that allows Kotlin developers to leverage the full power of Spark 4.0 idiomatically and safely.

---

## 2. Problem Statement

The introduction of the Spark Connect architecture in Spark 4.0 fundamentally changes how client applications interact with the Spark driver and executors. This client-server model introduces new challenges related to code and data serialization that did not exist in the traditional, single-JVM driver model.

1.  **Lack of Idiomatic Support in Core Spark:** The default Spark API for JVM languages (`Encoders.bean()`) relies on Java Bean conventions and reflection. This creates an "impedance mismatch" with modern Kotlin, failing to properly support features like primary constructors, non-nullable types, and sealed class hierarchies, often leading to verbose code or unexpected runtime failures.

2.  **Incompatibility of Existing Solutions:** The `kotlin-spark-api`, which solved these issues for previous Spark versions, is now architecturally incompatible. Its mechanisms for creating `Dataset` encoders and handling lambda functions (UDFs) assume a single-process environment and are not designed to be serialized and sent to a remote Spark server.

3.  **Developer Experience Gap:** Without an updated solution, Kotlin developers on Spark 4.0 are forced to write non-idiomatic, boilerplate-heavy code, sacrificing the type safety and conciseness that are primary benefits of the language. This creates a significant barrier to adoption for the modern data engineering ecosystem.

---

## 3. Primary Aim

The primary aim of this thesis is to **update and adapt the `kotlin-spark-api` to be fully compatible with the Spark 4.0 and Spark Connect architecture**, enabling developers to write safe, concise, and idiomatic Kotlin for distributed data processing.

---

## 4. Research and Development Plan

The project will be executed in four distinct phases, building from a foundational analysis to a validated software artifact.

#### Phase 1: Baseline Analysis & Test Suite Creation (Completed)

This foundational phase establishes a clear, measurable definition of "idiomatic Kotlin support" and serves as the specification for the project's final goal.

*   **Action:** Create a comprehensive test suite where each of Kotlin's key class and type features is defined in its most idiomatic form.
*   **Details:** For each feature (e.g., `data class`, `sealed interface`, `value class`, `enum`), create a test case that attempts to use it with the default Spark Connect API (`spark.createDataFrame`, `spark.createDataset`).
*   **Outcome:** A suite of tests that precisely documents the successes and failures of the out-of-the-box Spark Connect client. This suite forms the **acceptance criteria** for the entire project. The `tests.md` file serves as the human-readable report of this phase.

#### Phase 2: Integration and Gap Analysis of `kotlin-spark-api`

This phase will quantify the "gap" between the existing idiomatic API and the new Spark Connect architecture.

*   **Action:** Integrate the existing `kotlin-spark-api` into the project and refactor the test suite from Phase 1 to use its idiomatic helpers (e.g., `toDS()`, `.filter{}`, `.map{}`).
*   **Details:** Execute the refactored tests. It is hypothesized that a majority of tests will fail due to architectural incompatibilities. Each failure will be categorized and documented.
*   **Expected Failure Points:**
    1.  **Encoder Serialization:** The `kotlin-reflect` based encoder from the library will likely fail to serialize and transfer from the client to the Spark server.
    2.  **Lambda/UDF Serialization:** The mechanism used to convert Kotlin lambdas (`{ it.age > 30 }`) into Spark UDFs is not designed for Spark Connect's remote execution model.
    3.  **Client-Server Logic:** The library may contain invalid assumptions about direct access to the `SparkContext` or other driver-side components.

#### Phase 3: Re-implementation and Adaptation

This is the core engineering phase of the project, focused on fixing the issues identified in Phase 2.

*   **Action:** Systematically re-implement the failing components of `kotlin-spark-api` to be compatible with Spark Connect.
*   **Key Tasks:**
    1.  **Redesign the Encoder Mechanism:** Investigate how to make the Kotlin-aware encoder serializable. This may involve generating a lower-level, serializable representation of the object mapping (a "blueprint") on the client, which can be interpreted on the server, rather than sending the reflection logic itself.
    2.  **Re-implement UDF Wrappers:** Rewrite the lambda-handling logic to use the official `spark-connect-client` API for creating and registering UDFs, ensuring the function bytecode and its closure are correctly captured and sent to the server.
    3.  **Decouple Client/Server Code:** Audit the library for any logic that violates the Spark Connect client-server boundary and refactor it to use only the public remote API.

#### Phase 4: Validation and Documentation

This final phase validates the success of the project against the initial goals.

*   **Action:** Execute the complete, idiomatically-written test suite from Phase 2 against the newly adapted `kotlin-spark-api`.
*   **Success Criteria:** All tests must pass, demonstrating that idiomatic Kotlin code now runs correctly and safely on the Spark Connect architecture. Manual inspection of Spark UI and job execution plans will be used to verify proper distributed execution.
*   **Outcome:**
    *   Update the `README.md` to reflect the new, working solution.
    *   Produce a final report (the thesis) detailing the challenges, the implemented solutions, and performance considerations.

---

## 5. Expected Outcomes & Deliverables

1.  **A Forked and Updated `kotlin-spark-api` Library:** A publicly available Git repository containing the modified, Spark Connect-compatible library.
2.  **A Comprehensive Test Suite:** The suite of Kotlin class tests, serving as a benchmark for Kotlin compatibility in Spark.
3.  **Technical Documentation:** Updated `README.md` and other documents (`tests.md`, `future.md`) explaining how to use the library and the technical details of the implementation.
4.  **Thesis Document:** A full academic paper detailing the project's motivation, methodology, results, and contributions to the Spark and Kotlin ecosystems.
