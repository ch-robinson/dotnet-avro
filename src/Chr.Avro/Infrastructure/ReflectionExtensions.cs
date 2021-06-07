namespace Chr.Avro.Infrastructure
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;

    /// <summary>
    /// Provides methods that simplify common reflection operations.
    /// </summary>
    internal static class ReflectionExtensions
    {
        /// <summary>
        /// Gets an attribute on a <see cref="MemberInfo" />.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> of attribute to get.
        /// </typeparam>
        /// <param name="member">
        /// A <see cref="MemberInfo" /> object to search for <typeparamref name="T" />.
        /// </param>
        /// <returns>
        /// The <typeparamref name="T" /> if present; <c>null</c> otherwise.
        /// </returns>
        public static T? GetAttribute<T>(this MemberInfo member)
            where T : Attribute
        {
            return member.GetCustomAttributes(typeof(T), true)
                .OfType<T>()
                .SingleOrDefault();
        }

        /// <summary>
        /// Gets an attribute on a <see cref="Type" />.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> of attribute to get.
        /// </typeparam>
        /// <param name="type">
        /// A <see cref="Type" /> object to search for <typeparamref name="T" />.
        /// </param>
        /// <returns>
        /// The <typeparamref name="T" /> if present; <c>null</c> otherwise.
        /// </returns>
        public static T? GetAttribute<T>(this Type type)
            where T : Attribute
        {
            return type.GetCustomAttributes(typeof(T), true)
                .OfType<T>()
                .SingleOrDefault();
        }

        /// <summary>
        /// Gets the key and value <see cref="Type" />s of a dictionary <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> object that describes a generic dictionary.
        /// </param>
        /// <returns>
        /// If <paramref name="type" /> implements (or is) <see cref="IDictionary{TKey,TValue}" />,
        /// its key and value arguments; <c>null</c> otherwise.
        /// </returns>
        public static (Type Key, Type Value)? GetDictionaryTypes(this Type type)
        {
            var pairType = type.GetEnumerableType();

            if (pairType != null && pairType.IsGenericType && pairType.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
            {
                return (
                    pairType.GetGenericArguments().ElementAt(0),
                    pairType.GetGenericArguments().ElementAt(1));
            }

            return null;
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
        public static Type? GetEnumerableType(this Type type)
        {
            return new[] { type }
                .Concat(type.GetInterfaces())
                .SingleOrDefault(candidate => candidate.IsGenericType && candidate.GetGenericTypeDefinition() == typeof(IEnumerable<>))?
                .GetGenericArguments()?
                .ElementAt(0);
        }

        /// <summary>
        /// Creates an instance of <typeparamref name="T" /> without any fields or properties
        /// initialized.
        /// </summary>
        /// <typeparam name="T">
        /// A <see cref="Type" /> that can be safely created without initialization.
        /// </typeparam>
        /// <returns>
        /// A zeroed instance of <typeparamref name="T" />.
        /// </returns>
        public static T GetUninitializedInstance<T>()
            where T : notnull
        {
            return (T)FormatterServices.GetUninitializedObject(typeof(T));
        }
    }
}
