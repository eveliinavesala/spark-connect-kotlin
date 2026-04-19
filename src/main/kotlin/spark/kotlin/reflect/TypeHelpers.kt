package spark.kotlin.reflect

import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.KTypeParameter
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
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
    val typeArgs = kType.arguments.mapNotNull { it.type }
    return typeParams.zip(typeArgs).toMap()
}

/**
 * Substitutes type parameters in [kType] using [argMap].
 * - Direct type parameter (e.g. T): replaced by the mapped concrete type. Usage-site
 *   nullability is preserved (T? with T=String gives String?).
 * - Parameterized type (e.g. List<T>): arguments are resolved recursively.
 * - Non-generic type or empty map: returned unchanged.
 */
internal fun resolveTypeParam(
    kType: KType,
    argMap: Map<KTypeParameter, KType>,
): KType {
    val classifier = kType.classifier
    return when {
        argMap.isEmpty() -> {
            kType
        }

        classifier is KTypeParameter -> {
            val resolved = argMap[classifier] ?: return kType
            if (kType.isMarkedNullable && !resolved.isMarkedNullable) {
                resolved.jvmErasure.createType(resolved.arguments, nullable = true)
            } else {
                resolved
            }
        }

        kType.arguments.isEmpty() -> {
            kType
        }

        else -> {
            val resolvedArgs =
                kType.arguments.map { proj ->
                    val t = proj.type ?: return@map proj
                    KTypeProjection(proj.variance, resolveTypeParam(t, argMap))
                }
            kType.jvmErasure.createType(resolvedArgs, kType.isMarkedNullable)
        }
    }
}
