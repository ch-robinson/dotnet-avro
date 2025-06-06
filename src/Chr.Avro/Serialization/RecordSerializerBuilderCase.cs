namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text.RegularExpressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="RecordSchema" />.
    /// </summary>
    public abstract class RecordSerializerBuilderCase : SerializerBuilderCase
    {
        private static readonly Regex FuzzyCharacters = new(@"[^A-Za-z0-9]");

        /// <summary>
        /// Builds an <see cref="Expression" /> representing a dynamic get of a field or
        /// property.
        /// </summary>
        /// <param name="object">
        /// An <see cref="Expression" /> representing an object of an unknown type.
        /// </param>
        /// <param name="name">
        /// The name of the member to get.
        /// </param>
        /// <param name="defaultValue">
        /// An value to return when no member matching <paramref name="name" /> is present. If
        /// null, <see cref="MissingMemberException" /> will be thrown at serialization time if the
        /// member does not exist.
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing a dynamic get of <paramref name="name" />
        /// on <paramref name="object" />.
        /// </returns>
        protected virtual Expression BuildDynamicGet(Expression @object, string name, DefaultValue? defaultValue = default)
        {
            var binder = Binders.GetMember(name, defaultValue);

            return Expression.Dynamic(binder, binder.ReturnType, @object);
        }

        /// <summary>
        /// Finds at most one type member that matches a <see cref="RecordField" />.
        /// </summary>
        /// <param name="field">
        /// A record field.
        /// </param>
        /// <param name="type">
        /// The type to inspect.
        /// </param>
        /// <param name="memberVisibility">
        /// The binding flags used to select fields and properties.
        /// </param>
        /// <returns>
        /// A match if <see cref="IsMatch(RecordField, MemberInfo)" /> returns <c>true</c> for at
        /// most one type member; <c>null</c> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> has multiple members that match
        /// <paramref name="field" />.
        /// </exception>
        protected virtual MemberInfo? GetMatch(RecordField field, Type type, BindingFlags memberVisibility)
        {
            var matches = type.GetMembers(memberVisibility)
                .Where(member => IsMatch(field, member))
                .ToList();

            if (matches.Count > 1)
            {
                throw new UnsupportedTypeException(type, $"Multiple members ({string.Join(", ", matches.Select(m => m.Name))}) on {type} match the {field.Name} field.");
            }

            return matches.FirstOrDefault();
        }

        /// <summary>
        /// Determines whether a <see cref="RecordField" /> is a match for a type member.
        /// </summary>
        /// <param name="field">
        /// A record field.
        /// </param>
        /// <param name="member">
        /// The member to compare.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="member" /> is a match; <c>false</c> otherwise.
        /// </returns>
        protected virtual bool IsMatch(RecordField field, MemberInfo member)
        {
            if (member.MemberType != MemberTypes.Field && member.MemberType != MemberTypes.Property)
            {
                return false;
            }

            if (member.DeclaringType.HasAttribute<DataContractAttribute>())
            {
                if (member.GetAttribute<DataMemberAttribute>() is DataMemberAttribute memberAttribute)
                {
                    if (string.IsNullOrEmpty(memberAttribute.Name))
                    {
                        return IsMatch(field, member.Name);
                    }
                    else
                    {
                        return field.Name == memberAttribute.Name;
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
                    && IsMatch(field, member.Name);
            }
        }

        /// <summary>
        /// Determines whether a <see cref="RecordField" /> name matches another name.
        /// </summary>
        /// <param name="field">
        /// A record field.
        /// </param>
        /// <param name="name">
        /// The name to compare.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="name" /> is a match; <c>false</c> otherwise.
        /// </returns>
        protected virtual bool IsMatch(RecordField field, string name)
        {
            return string.Equals(
                FuzzyCharacters.Replace(field.Name, string.Empty),
                FuzzyCharacters.Replace(name, string.Empty),
                StringComparison.InvariantCultureIgnoreCase);
        }
    }
}
