namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Collections.ObjectModel;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

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
        protected override Expression BuildConversion(Expression value, Type target)
        {
            if (!value.Type.IsArray && (target.IsArray || target.IsAssignableFrom(typeof(ArraySegment<>).MakeGenericType(target.GenericTypeArguments))))
            {
                var toArray = value.Type
                    .GetMethod("ToArray", Type.EmptyTypes);

                value = Expression.Call(value, toArray);
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

            return base.BuildConversion(value, target);
        }

        /// <summary>
        /// Builds an <see cref="Expression" /> that represents instantiating a new collection.
        /// </summary>
        /// <remarks>
        /// This method includes conditions to support deserializing to concrete collection types
        /// that ship with .NET.
        /// </remarks>
        /// <param name="resolution">
        /// An <see cref="ArrayResolution" /> containing information about the target <see cref="Type" />.
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the creation of a collection that can be
        /// converted to the <see cref="Type" /> described by <paramref name="resolution" />.
        /// </returns>
        protected virtual Expression BuildIntermediateCollection(ArrayResolution resolution)
        {
            if (resolution.Type.IsArray || resolution.Type.IsAssignableFrom(typeof(ArraySegment<>).MakeGenericType(resolution.ItemType)) || resolution.Type.IsAssignableFrom(typeof(ImmutableArray<>).MakeGenericType(resolution.ItemType)))
            {
                var createBuilder = typeof(ImmutableArray)
                    .GetMethod(nameof(ImmutableArray.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.ItemType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(ImmutableHashSet<>).MakeGenericType(resolution.ItemType)))
            {
                var createBuilder = typeof(ImmutableHashSet)
                    .GetMethod(nameof(ImmutableHashSet.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.ItemType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(ImmutableList<>).MakeGenericType(resolution.ItemType)))
            {
                var createBuilder = typeof(ImmutableList)
                    .GetMethod(nameof(ImmutableList.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.ItemType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(ImmutableSortedSet<>).MakeGenericType(resolution.ItemType)))
            {
                var createBuilder = typeof(ImmutableSortedSet)
                    .GetMethod(nameof(ImmutableSortedSet.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.ItemType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(HashSet<>).MakeGenericType(resolution.ItemType)))
            {
                return Expression.New(typeof(HashSet<>).MakeGenericType(resolution.ItemType).GetConstructor(Type.EmptyTypes));
            }

            if (resolution.Type.IsAssignableFrom(typeof(SortedSet<>).MakeGenericType(resolution.ItemType)))
            {
                return Expression.New(typeof(SortedSet<>).MakeGenericType(resolution.ItemType).GetConstructor(Type.EmptyTypes));
            }

            if (resolution.Type.IsAssignableFrom(typeof(Collection<>).MakeGenericType(resolution.ItemType)))
            {
                return Expression.New(typeof(Collection<>).MakeGenericType(resolution.ItemType).GetConstructor(Type.EmptyTypes));
            }

            return Expression.New(typeof(List<>).MakeGenericType(resolution.ItemType).GetConstructor(Type.EmptyTypes));
        }

        /// <summary>
        /// Gets a constructor that can be used to instantiate a collection type.
        /// </summary>
        /// <param name="resolution">
        /// An <see cref="ArrayResolution" /> containing information about the collection
        /// <see cref="Type" />.
        /// </param>
        /// <returns>
        /// A <see cref="ConstructorResolution" /> from <paramref name="resolution" /> if one
        /// matches; <c>null</c> otherwise.
        /// </returns>
        protected virtual ConstructorResolution? FindEnumerableConstructor(ArrayResolution resolution)
        {
            return resolution.Constructors
                .Where(constructor => constructor.Parameters.Count == 1)
                .FirstOrDefault(constructor => constructor.Parameters.First().Parameter.ParameterType
                    .IsAssignableFrom(typeof(IEnumerable<>).MakeGenericType(resolution.ItemType)));
        }
    }
}
