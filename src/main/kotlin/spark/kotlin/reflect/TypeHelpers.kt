package spark.kotlin.reflect

import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.KTypeParameter
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure

/**
 * Returns all concrete (non-sealed) leaf subclasses in the sealed hierarchy rooted at this class.
 * Intermediate sealed classes (e.g. a sealed interface that extends another sealed interface)
 * are recursed into but not included themselves.
 */
internal fun KClass<*>.allLeafSubclasses(): List<KClass<*>> =
    sealedSubclasses.flatMap { sub ->
        if (sub.isSealed) sub.allLeafSubclasses() else listOf(sub)
    }

/**
 * Builds a map from each type parameter of [kType]'s class to the concrete type argument
 * supplied at the call site. E.g. for Box<String>, returns {T → String}.
 * Star projections (whose type is null) are omitted.
 */
internal fun buildTypeArgMap(kType: KType): Map<KTypeParameter, KType> {
    val typeParams = kType.jvmErasure.typeParameters
    val typeArgs   = kType.arguments.mapNotNull { it.type }
    return typeParams.zip(typeArgs).toMap()
}

/**
 * Substitutes type parameters in [kType] using [argMap].
 * - Direct type parameter (e.g. T): replaced by the mapped concrete type. Usage-site
 *   nullability is preserved (T? with T=String gives String?).
 * - Parameterized type (e.g. List<T>): arguments are resolved recursively.
 * - Non-generic type or empty map: returned unchanged.
 */
internal fun resolveTypeParam(kType: KType, argMap: Map<KTypeParameter, KType>): KType {
    if (argMap.isEmpty()) return kType
    val classifier = kType.classifier
    if (classifier is KTypeParameter) {
        val resolved = argMap[classifier] ?: return kType
        return if (kType.isMarkedNullable && !resolved.isMarkedNullable)
            resolved.jvmErasure.createType(resolved.arguments, nullable = true)
        else resolved
    }
    if (kType.arguments.isEmpty()) return kType
    val resolvedArgs = kType.arguments.map { proj ->
        val t = proj.type ?: return@map proj
        KTypeProjection(proj.variance, resolveTypeParam(t, argMap))
    }
    return kType.jvmErasure.createType(resolvedArgs, kType.isMarkedNullable)
}
