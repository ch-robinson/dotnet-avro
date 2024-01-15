namespace Chr.Avro.Serialization
{
    using System;
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
