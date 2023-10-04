namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

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
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target.Assembly == typeof(ImmutableInterlocked).Assembly)
            {
                var toImmutable = value.Type
                    .GetMethod("ToImmutable", Type.EmptyTypes);

                value = Expression.Call(value, toImmutable);
            }

            return base.BuildStaticConversion(value, target);
        }

        /// <summary>
        /// Builds an <see cref="Expression" /> that represents instantiating a new dictionary.
        /// </summary>
        /// <remarks>
        /// This method includes conditions to support deserializing to concrete dictionary types
        /// that ship with .NET.
        /// </remarks>
        /// <param name="type">
        /// A dictionary <see cref="Type" />.
        /// </param>
        /// <param name="keyType">
        /// The key <see cref="Type" /> of <paramref name="type" />.
        /// </param>
        /// <param name="valueType">
        /// The value <see cref="Type" /> of <paramref name="type" />.
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the creation of a dictionary that can be
        /// converted to <paramref name="type" />.
        /// </returns>
        protected virtual Expression BuildIntermediateDictionary(Type type, Type keyType, Type valueType)
        {
            if (type.IsAssignableFrom(typeof(Dictionary<,>).MakeGenericType(keyType, valueType)))
            {
                // prefer Dictionary<,> since it's the most obvious surrogate type for IDictionary<,>
            }
            else if (type.IsAssignableFrom(typeof(ImmutableDictionary<,>).MakeGenericType(keyType, valueType)))
            {
                var createBuilder = typeof(ImmutableDictionary)
                    .GetMethod(nameof(ImmutableDictionary.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(keyType, valueType);

                return Expression.Call(null, createBuilder);
            }
            else if (type.IsAssignableFrom(typeof(ImmutableSortedDictionary<,>).MakeGenericType(keyType, valueType)))
            {
                var createBuilder = typeof(ImmutableSortedDictionary)
                    .GetMethod(nameof(ImmutableSortedDictionary.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(keyType, valueType);

                return Expression.Call(null, createBuilder);
            }
            else if (type.IsAssignableFrom(typeof(SortedDictionary<,>).MakeGenericType(keyType, valueType)))
            {
                return Expression.New(typeof(SortedDictionary<,>).MakeGenericType(keyType, valueType).GetConstructor(Type.EmptyTypes));
            }
            else if (type.IsAssignableFrom(typeof(SortedList<,>).MakeGenericType(keyType, valueType)))
            {
                return Expression.New(typeof(SortedList<,>).MakeGenericType(keyType, valueType).GetConstructor(Type.EmptyTypes));
            }

            return Expression.New(typeof(Dictionary<,>).MakeGenericType(keyType, valueType).GetConstructor(Type.EmptyTypes));
        }

        /// <summary>
        /// Gets a constructor that can be used to instantiate a dictionary type.
        /// </summary>
        /// <param name="type">
        /// A dictionary <see cref="Type" />.
        /// </param>
        /// <returns>
        /// A <see cref="ConstructorInfo" /> from <paramref name="type" /> if one matches;
        /// <c>null</c> otherwise.
        /// </returns>
        protected virtual ConstructorInfo? GetDictionaryConstructor(Type type)
        {
            var (keyType, valueType) = type.GetDictionaryTypes() ?? throw new ArgumentException($"{type} is not a dictionary type.");

            return type.GetConstructors()
                .Where(constructor => constructor.GetParameters().Length == 1)
                .FirstOrDefault(constructor => constructor.GetParameters().First().ParameterType
                    .IsAssignableFrom(typeof(IDictionary<,>).MakeGenericType(keyType, valueType)));
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
    }
}
