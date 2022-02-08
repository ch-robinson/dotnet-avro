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
    /// Provides a base implementation for serializer builder cases that match <see cref="ArraySchema" />.
    /// </summary>
    public abstract class ArraySerializerBuilderCase : SerializerBuilderCase
    {
        /// <inheritdoc />
        protected override Expression BuildDynamicConversion(Expression value, Type target)
        {
            if (target.GetEnumerableType() is Type itemType)
            {
                var collectionType = typeof(ICollection<>).MakeGenericType(itemType);
                var enumerableType = typeof(IEnumerable<>).MakeGenericType(itemType);

                var toList = typeof(Enumerable)
                    .GetMethod(nameof(Enumerable.ToList))
                    .MakeGenericMethod(itemType);

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
                                    typeof(ArraySerializerBuilderCase),
                                    new[]
                                    {
                                        CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.IsStaticType, null),
                                        CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null),
                                    }),
                                typeof(object),
                                Expression.Constant(typeof(ArraySerializerBuilderCase)),
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
            if (target.GetEnumerableType() is Type itemType)
            {
                var collectionType = typeof(ICollection<>).MakeGenericType(itemType);
                var enumerableType = typeof(IEnumerable<>).MakeGenericType(itemType);

                if (!collectionType.IsAssignableFrom(value.Type))
                {
                    var toList = typeof(Enumerable)
                        .GetMethod(nameof(Enumerable.ToList))
                        .MakeGenericMethod(itemType);

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
        /// Gets the item <see cref="Type" /> of an enumerable <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> object that describes a generic enumerable.
        /// </param>
        /// <returns>
        /// If <paramref name="type" /> implements (or is) <see cref="IEnumerable{T}" />, its type
        /// argument; <c>null</c> otherwise.
        /// </returns>
        protected virtual Type? GetEnumerableType(Type type)
        {
            return type.GetEnumerableType();
        }

        /// <summary>
        /// Creates an enumerable that ensures items from the source enumerable are boxed.
        /// </summary>
        /// <param name="enumerable">
        /// An enumerable of any type.
        /// </param>
        /// <returns>
        /// An enumerable whose items are guaranteed to be boxed.
        /// </returns>
        private static IEnumerable<object?> Cast<T>(IEnumerable<T> enumerable)
        {
            foreach (object? item in enumerable)
            {
                yield return item;
            }
        }
    }
}
