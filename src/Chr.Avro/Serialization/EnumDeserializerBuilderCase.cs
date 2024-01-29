namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text.RegularExpressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="EnumSchema" />.
    /// </summary>
    public abstract class EnumDeserializerBuilderCase : DeserializerBuilderCase
    {
        private static readonly Regex FuzzyCharacters = new(@"[^A-Za-z0-9]");

        /// <summary>
        /// Finds at most one type member that matches an enum symbol.
        /// </summary>
        /// <param name="symbol">
        /// An enum symbol.
        /// </param>
        /// <param name="type">
        /// The type to inspect.
        /// </param>
        /// <returns>
        /// A match if <see cref="IsMatch(string, MemberInfo)" /> returns <c>true</c> for at
        /// most one type member; <c>null</c> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> has multiple members that match
        /// <paramref name="symbol" />.
        /// </exception>
        protected virtual MemberInfo? GetMatch(string symbol, Type type)
        {
            // enum fields will always be public static, so no need to expose binding flags:
            var matches = type.GetFields(BindingFlags.Public | BindingFlags.Static)
                .Where(member => IsMatch(symbol, member))
                .ToList();

            if (matches.Count > 1)
            {
                throw new UnsupportedTypeException(type, $"Multiple members ({string.Join(", ", matches.Select(m => m.Name))}) on {type} match the {symbol} symbol.");
            }

            return matches.FirstOrDefault();
        }

        /// <summary>
        /// Determines whether an enum symbol is a match for a type member.
        /// </summary>
        /// <param name="symbol">
        /// An enum symbol.
        /// </param>
        /// <param name="member">
        /// The member to compare.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="member" /> is a match; <c>false</c> otherwise.
        /// </returns>
        protected virtual bool IsMatch(string symbol, MemberInfo member)
        {
            if (member.MemberType != MemberTypes.Field)
            {
                return false;
            }

            if (member.DeclaringType.HasAttribute<DataContractAttribute>())
            {
                if (member.GetAttribute<EnumMemberAttribute>() is EnumMemberAttribute memberAttribute)
                {
                    if (string.IsNullOrEmpty(memberAttribute.Value))
                    {
                        return IsMatch(symbol, member.Name);
                    }
                    else
                    {
                        return symbol == memberAttribute.Value;
                    }
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return !(member.HasAttribute<IgnoreDataMemberAttribute>() || member.HasAttribute<NonSerializedAttribute>())
                    && IsMatch(symbol, member.Name);
            }
        }

        /// <summary>
        /// Determines whether an enum symbol matches another name.
        /// </summary>
        /// <param name="symbol">
        /// An enum symbol.
        /// </param>
        /// <param name="name">
        /// The name to compare.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="name" /> is a match; <c>false</c> otherwise.
        /// </returns>
        protected virtual bool IsMatch(string symbol, string name)
        {
            return string.Equals(
                FuzzyCharacters.Replace(symbol, string.Empty),
                FuzzyCharacters.Replace(name, string.Empty),
                StringComparison.InvariantCultureIgnoreCase);
        }
    }
}
