namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Collections.ObjectModel;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="ArraySchema" />.
    /// </summary>
    public abstract class ArrayDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <remarks>
        /// This override includes additional conditions to handle conversions to arrays and other
        /// collection types. If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target.IsAssignableFrom(value.Type))
            {
                // since the next check (ArraySegment<T>) matches IEnumerable<> and ICollection<>,
                // fall through to avoid generating an unnecessary .ToArray
            }
            else if (!value.Type.IsArray && (target.IsArray || target.IsAssignableFrom(typeof(ArraySegment<>).MakeGenericType(target.GenericTypeArguments))))
            {
                var toArray = value.Type
                    .GetMethod("ToArray", Type.EmptyTypes);

                value = Expression.Call(value, toArray);
#if !NET6_0_OR_GREATER

                // no implicit conversion from T[] to ArraySegment<T> exists on .NET Framework, so
                // generate a constructor explicitly:
                if (!target.IsArray && target == typeof(ArraySegment<>).MakeGenericType(target.GenericTypeArguments))
                {
                    var arraySegmentConstructor = typeof(ArraySegment<>)
                        .MakeGenericType(target.GenericTypeArguments)
                        .GetConstructor(new[] { target.GenericTypeArguments[0].MakeArrayType() });

                    value = Expression.New(arraySegmentConstructor, value);
                }
#endif
            }
            else if (target.Assembly == typeof(ImmutableInterlocked).Assembly)
            {
                if (target.IsAssignableFrom(typeof(ImmutableQueue<>).MakeGenericType(target.GenericTypeArguments)))
                {
                    var createRange = typeof(ImmutableQueue)
                        .GetMethod(nameof(ImmutableQueue.CreateRange))
                        .MakeGenericMethod(target.GenericTypeArguments);

                    value = Expression.Call(null, createRange, value);
                }
                else if (target.IsAssignableFrom(typeof(ImmutableStack<>).MakeGenericType(target.GenericTypeArguments)))
                {
                    var createRange = typeof(ImmutableStack)
                        .GetMethod(nameof(ImmutableStack.CreateRange))
                        .MakeGenericMethod(target.GenericTypeArguments);

                    value = Expression.Call(null, createRange, value);
                }
                else
                {
                    var toImmutable = value.Type
                        .GetMethod("ToImmutable", Type.EmptyTypes);

                    value = Expression.Call(value, toImmutable);
                }
            }
            else if (target.IsAssignableFrom(typeof(ReadOnlyCollection<>).MakeGenericType(target.GenericTypeArguments)))
            {
                var readOnlyCollectionConstructor = typeof(ReadOnlyCollection<>)
                    .MakeGenericType(target.GenericTypeArguments)
                    .GetConstructor(new[] { typeof(List<>).MakeGenericType(target.GenericTypeArguments) });

                value = Expression.New(readOnlyCollectionConstructor, value);
            }
            else if (target.IsAssignableFrom(typeof(ReadOnlyObservableCollection<>).MakeGenericType(target.GenericTypeArguments)))
            {
                var observableCollectionType = typeof(ObservableCollection<>)
                    .MakeGenericType(target.GenericTypeArguments);

                var observableCollectionConstructor = observableCollectionType
                    .GetConstructor(new[] { typeof(List<>).MakeGenericType(target.GenericTypeArguments) });

                var readOnlyCollectionObservableConstructor = typeof(ReadOnlyObservableCollection<>)
                    .MakeGenericType(target.GenericTypeArguments)
                    .GetConstructor(new[] { observableCollectionType });

                value = Expression.New(
                    readOnlyCollectionObservableConstructor,
                    Expression.New(observableCollectionConstructor, value));
            }

            return base.BuildStaticConversion(value, target);
        }

        /// <summary>
        /// Builds an <see cref="Expression" /> that represents instantiating a new collection.
        /// </summary>
        /// <remarks>
        /// This method includes conditions to support deserializing to concrete collection types
        /// that ship with .NET.
        /// </remarks>
        /// <param name="type">
        /// An enumerable <see cref="Type" />.
        /// </param>
        /// <param name="itemType">
        /// The item <see cref="Type" /> of <paramref name="type" />.
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the creation of a collection that can be
        /// converted to <paramref name="type" />.
        /// </returns>
        protected virtual Expression BuildIntermediateCollection(Type type, Type itemType)
        {
            if (type.IsAssignableFrom(typeof(List<>).MakeGenericType(itemType)))
            {
                // prefer List<> since it's the most obvious surrogate type for IEnumerable<>,
                // ICollection<>, etc.
            }
            else if (type.IsArray || type.IsAssignableFrom(typeof(ArraySegment<>).MakeGenericType(itemType)) || type.IsAssignableFrom(typeof(ImmutableArray<>).MakeGenericType(itemType)))
            {
                var createBuilder = typeof(ImmutableArray)
                    .GetMethod(nameof(ImmutableArray.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(itemType);

                return Expression.Call(null, createBuilder);
            }
            else if (type.IsAssignableFrom(typeof(ImmutableHashSet<>).MakeGenericType(itemType)))
            {
                var createBuilder = typeof(ImmutableHashSet)
                    .GetMethod(nameof(ImmutableHashSet.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(itemType);

                return Expression.Call(null, createBuilder);
            }
            else if (type.IsAssignableFrom(typeof(ImmutableList<>).MakeGenericType(itemType)))
            {
                var createBuilder = typeof(ImmutableList)
                    .GetMethod(nameof(ImmutableList.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(itemType);

                return Expression.Call(null, createBuilder);
            }
            else if (type.IsAssignableFrom(typeof(ImmutableSortedSet<>).MakeGenericType(itemType)))
            {
                var createBuilder = typeof(ImmutableSortedSet)
                    .GetMethod(nameof(ImmutableSortedSet.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(itemType);

                return Expression.Call(null, createBuilder);
            }
            else if (type.IsAssignableFrom(typeof(HashSet<>).MakeGenericType(itemType)))
            {
                return Expression.New(typeof(HashSet<>).MakeGenericType(itemType).GetConstructor(Type.EmptyTypes));
            }
            else if (type.IsAssignableFrom(typeof(SortedSet<>).MakeGenericType(itemType)))
            {
                return Expression.New(typeof(SortedSet<>).MakeGenericType(itemType).GetConstructor(Type.EmptyTypes));
            }
            else if (type.IsAssignableFrom(typeof(Collection<>).MakeGenericType(itemType)))
            {
                return Expression.New(typeof(Collection<>).MakeGenericType(itemType).GetConstructor(Type.EmptyTypes));
            }

            return Expression.New(typeof(List<>).MakeGenericType(itemType).GetConstructor(Type.EmptyTypes));
        }

        /// <summary>
        /// Gets a constructor that can be used to instantiate a collection type.
        /// </summary>
        /// <param name="type">
        /// A collection <see cref="Type" />.
        /// </param>
        /// <returns>
        /// A <see cref="ConstructorInfo" /> from <paramref name="type" /> if one matches;
        /// <c>null</c> otherwise.
        /// </returns>
        protected virtual ConstructorInfo? GetCollectionConstructor(Type type)
        {
            var itemType = type.GetEnumerableType() ?? throw new ArgumentException($"{type} is not an enumerable type.");

            return type.GetConstructors()
                .Where(constructor => constructor.GetParameters().Length == 1)
                .FirstOrDefault(constructor => constructor.GetParameters().First().ParameterType
                    .IsAssignableFrom(typeof(IEnumerable<>).MakeGenericType(itemType)));
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
    }
}
