namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;
    using Microsoft.CSharp.RuntimeBinder;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="MapSchema" />.
    /// </summary>
    public abstract class MapSerializerBuilderCase : SerializerBuilderCase
    {
        /// <inheritdoc />
        protected override Expression BuildDynamicConversion(Expression value, Type target)
        {
            if (target.GetDictionaryTypes() is (Type keyType, Type valueType))
            {
                var pairType = typeof(KeyValuePair<,>).MakeGenericType(keyType, valueType);
                var collectionType = typeof(ICollection<>).MakeGenericType(pairType);
                var enumerableType = typeof(IEnumerable<>).MakeGenericType(pairType);

                var toList = typeof(Enumerable)
                    .GetMethod(nameof(Enumerable.ToList))
                    .MakeGenericMethod(pairType);

                return Expression.Convert(
                    Expression.Call(
                        null,
                        toList,
                        Expression.Convert(
                            Expression.Dynamic(
                                Binder.InvokeMember(
                                    CSharpBinderFlags.None,
                                    nameof(Cast),
                                    null,
                                    typeof(MapSerializerBuilderCase),
                                    new[]
                                    {
                                        CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.IsStaticType, null),
                                        CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null),
                                    }),
                                typeof(object),
                                Expression.Constant(typeof(MapSerializerBuilderCase)),
                                value),
                            enumerableType)),
                    collectionType);
            }
            else
            {
                return base.BuildDynamicConversion(value, target);
            }
        }

        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target.GetDictionaryTypes() is (Type keyType, Type valueType))
            {
                var pairType = typeof(KeyValuePair<,>).MakeGenericType(keyType, valueType);
                var collectionType = typeof(ICollection<>).MakeGenericType(pairType);
                var enumerableType = typeof(IEnumerable<>).MakeGenericType(pairType);

                if (!collectionType.IsAssignableFrom(value.Type))
                {
                    var toList = typeof(Enumerable)
                        .GetMethod(nameof(Enumerable.ToList))
                        .MakeGenericMethod(pairType);

                    value = Expression.Call(
                        null,
                        toList,
                        base.BuildStaticConversion(value, enumerableType));
                }

                return base.BuildStaticConversion(value, collectionType);
            }
            else
            {
                return base.BuildStaticConversion(value, target);
            }
        }

        /// <summary>
        /// Gets the item <see cref="Type" /> of a dictionary <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> object that describes a generic dictionary.
        /// </param>
        /// <returns>
        /// If <paramref name="type" /> implements (or is) <see cref="IEnumerable{T}" /> and the
        /// item type is <see cref="KeyValuePair{TKey, TValue}" />, its type arguments; <c>null</c>
        /// otherwise.
        /// </returns>
        protected virtual (Type Key, Type Value)? GetDictionaryTypes(Type type)
        {
            return type.GetDictionaryTypes();
        }

        /// <summary>
        /// Creates an enumerable that ensures items from the source enumerable are boxed.
        /// </summary>
        /// <param name="enumerable">
        /// An enumerable of key-value pairs of any type.
        /// </param>
        /// <returns>
        /// An enumerable of key-value pairs whose keys and values are guaranteed to be boxed.
        /// </returns>
        private static IEnumerable<KeyValuePair<object?, object?>> Cast<TKey, TValue>(
            IEnumerable<KeyValuePair<TKey, TValue>> enumerable)
        {
            foreach (var pair in enumerable)
            {
                yield return new KeyValuePair<object?, object?>(pair.Key, pair.Value);
            }
        }
    }
}
