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
        /// Searches for the <see cref="MemberInfo" /> on <paramref name="type" /> that matches the
        /// specified <see cref="MemberInfo" />.
        /// </summary>
        /// <remarks>
        /// This method is a stand-in for the <c>Type.GetMemberWithSameMetadataDefinitionAs</c>
        /// method available in .NET 6 and above. See
        /// <see href="https://github.com/dotnet/runtime/blob/v6.0.0/src/libraries/System.Private.CoreLib/src/System/Type.cs">the .NET runtime source</see>
        /// (also MIT licensed) for the reference implementation.
        /// </remarks>
        /// <param name="type">
        ///  A <see cref="Type" /> object to search for a matching member.
        /// </param>
        /// <param name="member">
        /// The <see cref="MemberInfo" /> to find on <paramref name="type" />.
        /// </param>
        /// <returns>
        /// An object representing the member on <paramref name="type" /> that matches the
        /// specified member.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// <paramref name="member" /> does not match a member on <paramref name="type" />.
        /// </exception>
        public static MemberInfo GetMemberWithSameMetadataDefinitionAs(this Type type, MemberInfo member)
        {
            const BindingFlags all = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance;

            foreach (var candidate in type.GetMembers(all))
            {
                if (candidate.HasSameMetadataDefinitionAs(member))
                {
                    return candidate;
                }
            }

            throw new ArgumentException($"A {nameof(MemberInfo)} that matches {member} could not be found.", nameof(member));
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

        /// <summary>
        /// Determines whether <see cref="MemberInfo" /> has <typeparamref name="T" /> as an
        /// attribute.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> of attribute to test for.
        /// </typeparam>
        /// <param name="member">
        /// A <see cref="MemberInfo" /> object to search for <typeparamref name="T" />.
        /// </param>
        /// <returns>
        /// <c>true</c> if <typeparamref name="T" /> is present; <c>false</c> otherwise.
        /// </returns>
        public static bool HasAttribute<T>(this MemberInfo member)
            where T : Attribute
        {
            return member.GetCustomAttributes(typeof(T), true)
                .Any();
        }

        /// <summary>
        /// Determines whether <paramref name="member" /> has the same metadata as
        /// <paramref name="other" />.
        /// </summary>
        /// <remarks>
        /// This method is a stand-in for the <c>MemberInfo.HasSameMetadataDefinitionAs</c>
        /// method available in .NET Standard 2.1 and above. See
        /// <see href="https://github.com/dotnet/runtime/blob/v6.0.0/src/coreclr/System.Private.CoreLib/src/System/Reflection/MemberInfo.Internal.cs">the .NET runtime source</see>
        /// (also MIT licensed) for the reference implementation.
        /// </remarks>
        /// <param name="member">
        /// A <see cref="MemberInfo" /> object.
        /// </param>
        /// <param name="other">
        /// The <see cref="MemberInfo" /> to compare to <paramref name="member" />.
        /// </param>
        /// <returns>
        /// <c>true</c> if the metadata definitions are equivalent; <c>false</c> otherwise.
        /// </returns>
        public static bool HasSameMetadataDefinitionAs(this MemberInfo member, MemberInfo other)
        {
            return member.MetadataToken == other.MetadataToken && member.Module.Equals(other.Module);
        }

        /// <summary>
        /// Gets a value that indicates whether <paramref name="type" /> represents a type
        /// parameter in the definition of a generic method.
        /// </summary>
        /// <remarks>
        /// This method is a stand-in for the <c>Type.IsGenericMethodParameter</c> property
        /// available in .NET Standard 2.1 and above. See
        /// <see href="https://github.com/dotnet/runtime/blob/v6.0.0/src/libraries/System.Private.CoreLib/src/System/Type.cs">the .NET runtime source</see>
        /// (also MIT licensed) for the reference implementation.
        /// </remarks>
        /// <param name="type">
        /// A <see cref="Type" /> object.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="type" /> represents a type parameter of a generic
        /// method definition; <c>false</c> otherwise.
        /// </returns>
        public static bool IsGenericMethodParameter(this Type type)
        {
            return type.IsGenericParameter && type.DeclaringMethod != null;
        }
    }
}
