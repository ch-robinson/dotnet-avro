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
        /// Gets the element <see cref="Type" /> of an enumerable <see cref="Type" />.
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
        /// Gets the underlying <see cref="Type" /> of a <see cref="Nullable{T}" />.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> object that may be a nullable value type.
        /// </param>
        /// <returns>
        /// If <paramref name="type" /> is a nullable value type, its underlying <see cref="Type" />;
        /// <paramref name="type" /> otherwise.
        /// </returns>
        public static Type GetUnderlyingType(this Type type)
        {
            return Nullable.GetUnderlyingType(type) ?? type;
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
