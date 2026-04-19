package classes

sealed interface IdiomaticResult {
    data class Success(
        val data: String,
    ) : IdiomaticResult

    data class Error(
        val error: String,
    ) : IdiomaticResult
}
