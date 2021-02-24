namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="MapSchema" />.
    /// </summary>
    public abstract class MapDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <remarks>
        /// This override includes additional conditions to handle conversions to dictionary types.
        /// If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildConversion(Expression value, Type target)
        {
            if (target.Assembly == typeof(ImmutableInterlocked).Assembly)
            {
                var toImmutable = value.Type
                    .GetMethod("ToImmutable", Type.EmptyTypes);

                value = Expression.Call(value, toImmutable);
            }

            return base.BuildConversion(value, target);
        }

        /// <summary>
        /// Builds an <see cref="Expression" /> that represents instantiating a new dictionary.
        /// </summary>
        /// <remarks>
        /// This method includes conditions to support deserializing to concrete dictionary types
        /// that ship with .NET.
        /// </remarks>
        /// <param name="resolution">
        /// A <see cref="MapResolution" /> containing information about the target <see cref="Type" />.
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the creation of a dictionary that can be
        /// converted to the <see cref="Type" /> described by <paramref name="resolution" />.
        /// </returns>
        protected virtual Expression BuildIntermediateDictionary(MapResolution resolution)
        {
            if (resolution.Type.IsAssignableFrom(typeof(ImmutableDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)))
            {
                var createBuilder = typeof(ImmutableDictionary)
                    .GetMethod(nameof(ImmutableDictionary.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.KeyType, resolution.ValueType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(ImmutableSortedDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)))
            {
                var createBuilder = typeof(ImmutableSortedDictionary)
                    .GetMethod(nameof(ImmutableSortedDictionary.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.KeyType, resolution.ValueType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(SortedDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)))
            {
                return Expression.New(typeof(SortedDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType).GetConstructor(Type.EmptyTypes));
            }

            if (resolution.Type.IsAssignableFrom(typeof(SortedList<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)))
            {
                return Expression.New(typeof(SortedList<,>).MakeGenericType(resolution.KeyType, resolution.ValueType).GetConstructor(Type.EmptyTypes));
            }

            return Expression.New(typeof(Dictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType).GetConstructor(Type.EmptyTypes));
        }

        /// <summary>
        /// Gets a constructor that can be used to instantiate a dictionary type.
        /// </summary>
        /// <param name="resolution">
        /// A <see cref="MapResolution" /> containing information about the dictionary
        /// <see cref="Type" />.
        /// </param>
        /// <returns>
        /// A <see cref="ConstructorResolution" /> from <paramref name="resolution" /> if one
        /// matches; <c>null</c> otherwise.
        /// </returns>
        protected virtual ConstructorResolution? FindDictionaryConstructor(MapResolution resolution)
        {
            return resolution.Constructors
                .Where(constructor => constructor.Parameters.Count == 1)
                .FirstOrDefault(constructor => constructor.Parameters.First().Parameter.ParameterType
                    .IsAssignableFrom(typeof(IDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)));
        }
    }
}
