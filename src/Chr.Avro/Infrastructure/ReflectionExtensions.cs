namespace Chr.Avro.Infrastructure
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
#if NET6_0_OR_GREATER
    using System.Runtime.CompilerServices;
#endif
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
        /// Gets all members on a type that should be considered for serialization, taking
        /// <see cref="DataContractAttribute"/> and <see cref="DataMemberAttribute" /> into account.
        /// </summary>
        /// <param name="type">
        /// A class or struct <see cref="Type" />.
        /// </param>
        /// <param name="attributes">
        /// Binding attributes that should be used to select members.
        /// </param>
        /// <returns>
        /// An enumerable of <see cref="MemberInfo" />s representing serializable members.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="type" /> is not an enum.
        /// </exception>
        public static IEnumerable<MemberInfo> GetDataMembers(this Type type, BindingFlags attributes)
        {
            return type.GetMembers(attributes)
                .Where(member => type.HasAttribute<DataContractAttribute>()
                    ? member.HasAttribute<DataMemberAttribute>()
                    : !(member.HasAttribute<IgnoreDataMemberAttribute>() ||
                        member.HasAttribute<NonSerializedAttribute>()));
        }

        /// <summary>
        /// Gets the name of a type member, taking <see cref="DataMemberAttribute" /> into
        /// account.
        /// </summary>
        /// <param name="member">
        /// A <see cref="MemberInfo" /> of a field or property.
        /// </param>
        /// <returns>
        /// The name of the member (as overridden by <see cref="DataMemberAttribute" /> if
        /// present).
        /// </returns>
        public static string GetDataMemberName(this MemberInfo member)
        {
            if (member.DeclaringType.HasAttribute<DataContractAttribute>()
                && member.GetAttribute<DataMemberAttribute>() is DataMemberAttribute memberAttribute
                && !string.IsNullOrEmpty(memberAttribute.Name))
            {
                return memberAttribute.Name;
            }
            else
            {
                return member.Name;
            }
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
        /// Gets all members on an enum type that should be considered for serialization, taking
        /// <see cref="DataContractAttribute"/> and <see cref="EnumMemberAttribute" /> into account.
        /// </summary>
        /// <param name="type">
        /// An enum <see cref="Type" />.
        /// </param>
        /// <returns>
        /// An enumerable of <see cref="MemberInfo" />s representing serializable enum members.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="type" /> is not an enum.
        /// </exception>
        public static IEnumerable<MemberInfo> GetEnumMembers(this Type type)
        {
            if (!type.IsEnum)
            {
                throw new ArgumentException($"{type.FullName} is not an enum type.", nameof(type));
            }

            return type.GetMembers(BindingFlags.Public | BindingFlags.Static)
                .Where(member => type.HasAttribute<DataContractAttribute>()
                    ? member.HasAttribute<EnumMemberAttribute>()
                    : !(member.HasAttribute<IgnoreDataMemberAttribute>() ||
                        member.HasAttribute<NonSerializedAttribute>()));
        }

        /// <summary>
        /// Gets the name of an enum member, taking <see cref="EnumMemberAttribute" /> into
        /// account.
        /// </summary>
        /// <param name="member">
        /// A <see cref="MemberInfo" /> declared by an enum <see cref="Type" />.
        /// </param>
        /// <returns>
        /// The name of the member (as overridden by <see cref="EnumMemberAttribute" /> if
        /// present).
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="member" /> is not declared by an enum type.
        /// </exception>
        public static string GetEnumMemberName(this MemberInfo member)
        {
            if (!member.DeclaringType.IsEnum)
            {
                throw new ArgumentException($"{member.DeclaringType.FullName} is not an enum type.", nameof(member));
            }

            if (member.DeclaringType.HasAttribute<DataContractAttribute>()
                && member.GetAttribute<EnumMemberAttribute>() is EnumMemberAttribute memberAttribute
                && !string.IsNullOrEmpty(memberAttribute.Value))
            {
                return memberAttribute.Value;
            }
            else
            {
                return member.Name;
            }
        }

        /// <summary>
        /// Gets the name of an enum member, taking <see cref="EnumMemberAttribute" /> into
        /// account.
        /// </summary>
        /// <param name="type">
        /// An enum <see cref="Type" />.
        /// </param>
        /// <param name="value">
        /// A value of <paramref name="type" />.
        /// </param>
        /// <returns>
        /// The name of the member (as overridden by <see cref="EnumMemberAttribute" /> if
        /// present).
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="type" /> is not an enum or <paramref name="value" /> is
        /// not of <paramref name="type" />.
        /// </exception>
        public static string GetEnumMemberName(this Type type, object value)
        {
            var name = Enum.GetName(type, value);
            var field = type.GetField(name);

            return field.GetEnumMemberName();
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
#if NET6_0_OR_GREATER
            return (T)RuntimeHelpers.GetUninitializedObject(typeof(T));
#else
            return (T)FormatterServices.GetUninitializedObject(typeof(T));
#endif
        }

        /// <summary>
        /// Gets the value of a member.
        /// </summary>
        /// <param name="member">
        /// A <see cref="MemberInfo" /> object to get the value from.
        /// </param>
        /// <param name="object">
        /// An object instance.
        /// </param>
        /// <returns>
        /// The value of <paramref name="member" /> on <paramref name="object" />.
        /// </returns>
        public static object GetValue(this MemberInfo member, object @object)
        {
            return member switch
            {
                FieldInfo field => field.GetValue(@object),
                PropertyInfo property => property.GetValue(@object),
                _ => throw new ArgumentException($"Unable to get a value from {member.Name}."),
            };
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
