# Class Types in Kotlin

This document provides an overview of various Kotlin class types, their JVM-wrapped counterparts, and how they behave when used with a Spark JVM client.

## Class

### Data class
* **Kotlin:**
  ```kotlin
  package classes
  
  data class IdiomaticDataClass(val name: String, val age: Int)
  ```

* **JVM:**
  ```kotlin
  package classes
  
  class JvmWrappedDataClass(val name: String, val age: Int) {
      override fun toString(): String {
          return "JvmWrappedDataClass(name=$name, age=$age)"
      }
  
      override fun equals(other: Any?): Boolean {
          if (this === other) return true
          if (javaClass != other?.javaClass) return false
  
          other as JvmWrappedDataClass
  
          if (name != other.name) return false
          if (age != other.age) return false
  
          return true
      }
  
      override fun hashCode(): Int {
          var result = name.hashCode()
          result = 31 * result + age
          return result
      }
  }
  ```

* **Test result to JVM client:**
  The test creates a DataFrame from a list of `IdiomaticDataClass` instances. The test asserts that the DataFrame has the correct number of rows and that the data in the first row is as expected. This demonstrates that Spark can correctly handle Kotlin data classes.
  ```kotlin
  package classes
  
  import org.apache.spark.sql.functions.col
  import org.junit.jupiter.api.Assertions.assertEquals
  import org.junit.jupiter.api.Test
  
  class IdiomaticDataClassTest : SparkTestBase() {
  
      @Test
      fun `test data class with spark`() {
          val data = listOf(
              IdiomaticDataClass("Alice", 30),
              IdiomaticDataClass("Bob", 40)
          )
          val df = spark.createDataFrame(data)
  
          assertEquals(2, df.count())
          assertEquals("Alice", df.first().getAs<String>("name"))
      }
  }
  ```

### Enum class
* **Kotlin:**
  ```kotlin
  package classes
  
  enum class IdiomaticEnum {
      SUCCESS,
      ERROR
  }
  ```

* **JVM:**
  ```kotlin
  package classes
  
  enum class JvmWrappedEnum {
      SUCCESS,
      ERROR
  }
  ```

* **Test result to JVM client:**
  The test creates a DataFrame from a list of `EnumData` objects, which contain an enum. The test asserts that the DataFrame has the correct number of rows and that the enum value is correctly converted to a string.
  ```kotlin
  package classes
  
  import org.junit.jupiter.api.Assertions.assertEquals
  import org.junit.jupiter.api.Test
  
  class IdiomaticEnumTest : SparkTestBase() {
  
      @Test
      fun `test enum with spark`() {
          val data = listOf(
              Pair("A", IdiomaticEnum.SUCCESS),
              Pair("B", IdiomaticEnum.ERROR)
          ).map { (key, value) -> EnumData(key, value) }
  
          val df = spark.createDataFrame(data)
  
          assertEquals(2, df.count())
          assertEquals("SUCCESS", df.first().getAs<String>("value"))
      }
  
      data class EnumData(val key: String, val value: IdiomaticEnum)
  }
  ```

### Sealed class
* **Kotlin:**
  ```kotlin
  package classes
  
  sealed class Result
  data class Success(val data: String) : Result()
  data class Error(val error: String) : Result()
  ```

* **JVM:**
  ```kotlin
  package classes
  
  abstract class JvmWrappedResult
  
  class JvmWrappedSuccess(val data: String) : JvmWrappedResult()
  
  class JvmWrappedError(val error: String) : JvmWrappedResult()
  ```

* **Test result to JVM client:**
  The test creates a DataFrame from a list of `Result` objects. The test asserts that the DataFrame has the correct number of rows and that the data in the first row is as expected. This demonstrates that Spark can correctly handle sealed classes.
  ```kotlin
  package classes
  
  import org.junit.jupiter.api.Assertions.assertEquals
  import org.junit.jupiter.api.Test
  
  class IdiomaticSealedClassTest : SparkTestBase() {
  
      @Test
      fun `test sealed class with spark`() {
          val data = listOf(
              Success("Data"),
              Error("An error occurred")
          )
  
          val df = spark.createDataFrame(data)
          df.show()
  
          assertEquals(2, df.count())
          assertEquals("Data", df.first().getAs<String>("data"))
      }
  }
  ```

### Sealed interface
* **Kotlin:**
  ```kotlin
  package classes

  sealed interface IdiomaticResult {
      data class Success(val data: String) : IdiomaticResult
      data class Error(val error: String) : IdiomaticResult
  }
  ```

* **JVM:**
  ```kotlin
  package classes

  interface JvmWrappedResultInterface

  class JvmWrappedSuccessInterface(val data: String) : JvmWrappedResultInterface

  class JvmWrappedErrorInterface(val error: String) : JvmWrappedResultInterface
  ```

* **Test result to JVM client:**
  The test creates a DataFrame from a list of `IdiomaticResult` objects. The test asserts that the DataFrame has the correct number of rows and that the data in the first row is as expected. This demonstrates that Spark can correctly handle sealed interfaces.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Assertions.assertEquals
  import org.junit.jupiter.api.Test

  class IdiomaticSealedInterfaceTest : SparkTestBase() {

      @Test
      fun `test sealed interface with spark`() {
          val data = listOf(
              IdiomaticResult.Success("Data"),
              IdiomaticResult.Error("An error occurred")
          )

          val df = spark.createDataFrame(data)
          df.show()

          assertEquals(2, df.count())
          assertEquals("Data", df.first().getAs<String>("data"))
      }
  }
  ```

### Abstract class
* **Kotlin:**
  ```kotlin
  package classes

  abstract class IdiomaticAbstractClass {
      abstract fun doSomething()
  }
  ```

* **JVM:**
  ```kotlin
  package classes

  abstract class JvmWrappedAbstractClass {
      abstract fun doSomething()
  }
  ```

* **Test result to JVM client:**
  The test creates a concrete implementation of the abstract class and calls the abstract method. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test
  import org.junit.jupiter.api.Assertions.assertTrue

  class IdiomaticAbstractClassTest {

      class ConcreteClass : IdiomaticAbstractClass() {
          var called = false
          override fun doSomething() {
              called = true
          }
      }

      @Test
      fun `test abstract class`() {
          val concrete = ConcreteClass()
          concrete.doSomething()
          assertTrue(concrete.called)
      }
  }
  ```

### Open class
* **Kotlin:**
  ```kotlin
  package classes

  open class IdiomaticOpenClass {
      fun doSomething() {
          println("Doing something")
      }
  }
  ```

* **JVM:**
  ```kotlin
  package classes

  class JvmWrappedOpenClass {
      fun doSomething() {
          println("Doing something")
      }
  }
  ```

* **Test result to JVM client:**
  The test creates an instance of the open class and calls a method. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test

  class IdiomaticOpenClassTest {

      @Test
      fun `test open class`() {
          val instance = IdiomaticOpenClass()
          instance.doSomething() // Prints "Doing something"
      }
  }
  ```

### Final class
* **Kotlin:**
  ```kotlin
  package classes

  class IdiomaticFinalClass {
      fun doSomething() {
          println("Doing something")
      }
  }
  ```

* **JVM:**
  ```kotlin
  package classes

  final class JvmWrappedFinalClass {
      fun doSomething() {
          println("Doing something")
      }
  }
  ```

* **Test result to JVM client:**
  The test creates an instance of the final class and calls a method. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test

  class IdiomaticFinalClassTest {

      @Test
      fun `test final class`() {
          val instance = IdiomaticFinalClass()
          instance.doSomething() // Prints "Doing something"
      }
  }
  ```

### Interface
* **Kotlin:**
  ```kotlin
  package classes

  interface IdiomaticInterface {
      fun doSomething()
  }
  ```

* **JVM:**
  ```kotlin
  package classes

  interface JvmWrappedInterface {
      fun doSomething()
  }
  ```

* **Test result to JVM client:**
  The test creates a concrete implementation of the interface and calls a method. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test
  import org.junit.jupiter.api.Assertions.assertTrue

  class IdiomaticInterfaceTest {

      class ConcreteImplementation : IdiomaticInterface {
          var called = false
          override fun doSomething() {
              called = true
          }
      }

      @Test
      fun `test interface`() {
          val concrete = ConcreteImplementation()
          concrete.doSomething()
          assertTrue(concrete.called)
      }
  }
  ```

### Object declaration
* **Kotlin:**
  ```kotlin
  package classes

  object IdiomaticObject {
      fun log(message: String) {
          println(message)
      }
  }
  ```

* **JVM:**
  ```kotlin
  package classes

  class JvmWrappedObject private constructor() {
      companion object {
          val INSTANCE = JvmWrappedObject()
      }

      fun log(message: String) {
          println(message)
      }
  }
  ```

* **Test result to JVM client:**
  The test calls a method on the object. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test

  class IdiomaticObjectTest {

      @Test
      fun `test object`() {
          IdiomaticObject.log("Hello from object")
      }
  }
  ```

### Companion object
* **Kotlin:**
  ```kotlin
  package classes

  class IdiomaticCompanionObject {
      companion object {
          fun create(): IdiomaticCompanionObject = IdiomaticCompanionObject()
      }
  }
  ```

* **JVM:**
  ```kotlin
  package classes

  class JvmWrappedCompanionObject {
      companion object {
          @JvmStatic
          fun create(): JvmWrappedCompanionObject = JvmWrappedCompanionObject()
      }
  }
  ```

* **Test result to JVM client:**
  The test calls a method on the companion object. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test

  class IdiomaticCompanionObjectTest {

      @Test
      fun `test companion object`() {
          val instance = IdiomaticCompanionObject.create()
      }
  }
  ```

### Anonymous object
* **Kotlin:**
  ```kotlin
  package classes

  fun getAnonymousObject() = object {
      val message = "Hello"
  }
  ```

* **JVM:**
  ```kotlin
  package classes

  class JvmWrappedAnonymousObject {
      private class MyAnonymousObject {
          val message = "Hello"
      }

      fun getAnonymousObject(): Any {
          return MyAnonymousObject()
      }
  }
  ```

* **Test result to JVM client:**
  The test calls a function that returns an anonymous object and asserts a property on the object. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test
  import org.junit.jupiter.api.Assertions.assertEquals

  class IdiomaticAnonymousObjectTest {

      @Test
      fun `test anonymous object`() {
          val anonymous = getAnonymousObject()
          assertEquals("Hello", anonymous.message)
      }
  }
  ```

### Value class
* **Kotlin:**
  ```kotlin
  package classes

  @JvmInline
  value class IdiomaticValueClass(val value: String)
  ```

* **JVM:**
  ```kotlin
  package classes

  // In the JVM, the IdiomaticValueClass is represented as a String.
  // The compiler will optimize away the wrapper class.
  typealias JvmWrappedValueClass = String
  ```

* **Test result to JVM client:**
  The test creates a DataFrame from a list of `IdiomaticValueClass` instances. The test asserts that the DataFrame has the correct number of rows and that the data in the first row is as expected. This demonstrates that Spark can correctly handle value classes.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test
  import org.junit.jupiter.api.Assertions.assertEquals

  class IdiomaticValueClassTest : SparkTestBase() {

      @Test
      fun `test value class with spark`() {
          val data = listOf(
              IdiomaticValueClass("A"),
              IdiomaticValueClass("B")
          )
          val df = spark.createDataFrame(data)

          assertEquals(2, df.count())
          assertEquals("A", df.first().getAs<String>("value"))
      }
  }
  ```

### Nested class
* **Kotlin:**
  ```kotlin
  package classes

  class IdiomaticOuterClass {
      class Nested {
          fun doSomething() {
              println("Doing something in nested class")
          }
      }
  }
  ```

* **JVM:**
  ```java
  // JvmWrappedNestedClass.java
  public class JvmWrappedOuterClass {
      static class Nested {
          void doSomething() {
              System.out.println("Doing something in nested class");
          }
      }
  }
  ```

* **Test result to JVM client:**
  The test creates an instance of the nested class and calls a method. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test

  class IdiomaticNestedClassTest {

      @Test
      fun `test nested class`() {
          val nested = IdiomaticOuterClass.Nested()
          nested.doSomething() // Prints "Doing something in nested class"
      }
  }
  ```

### Inner class
* **Kotlin:**
  ```kotlin
  package classes

  class IdiomaticOuter {
      private val message = "Hello from outer"

      inner class Inner {
          fun doSomething() {
              println(message)
          }
      }
  }
  ```

* **JVM:**
  ```java
  // JvmWrappedInnerClass.java
  public class JvmWrappedOuter {
      private String message = "Hello from outer";

      class Inner {
          void doSomething() {
              System.out.println(new JvmWrappedOuter().message);
          }
      }
  }
  ```

* **Test result to JVM client:**
  The test creates an instance of the inner class and calls a method. This test does not involve Spark.
  ```kotlin
  package classes

  import org.junit.jupiter.api.Test

  class IdiomaticInnerClassTest {

      @Test
      fun `test inner class`() {
          val outer = IdiomaticOuter()
          val inner = outer.Inner()
          inner.doSomething() // Prints "Hello from outer"
      }
  }
  ```
