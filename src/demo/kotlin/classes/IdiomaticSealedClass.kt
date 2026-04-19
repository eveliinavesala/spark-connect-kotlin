package classes

sealed class Result

data class Success(
    val data: String,
) : Result()

data class Error(
    val error: String,
) : Result()
