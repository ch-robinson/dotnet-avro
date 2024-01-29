namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text.RegularExpressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="RecordSchema" />.
    /// </summary>
    public abstract class RecordDeserializerBuilderCase : DeserializerBuilderCase
    {
        private static readonly Regex FuzzyCharacters = new(@"[^A-Za-z0-9]");

        /// <summary>
        /// Gets a constructor with a matching parameter for each <see cref="RecordField" /> in a
        /// <see cref="RecordSchema" />.
        /// </summary>
        /// <param name="type">
        /// An object or struct <see cref="Type" />.
        /// </param>
        /// <param name="schema">
        /// A <see cref="RecordSchema" /> to match to <paramref name="type" />.
        /// </param>
        /// <returns>
        /// A <see cref="ConstructorInfo" /> from <paramref name="type" /> if one matches;
        /// <c>null</c> otherwise.
        /// </returns>
        protected virtual ConstructorInfo? GetRecordConstructor(Type type, RecordSchema schema)
        {
            return type.GetConstructors().FirstOrDefault(constructor =>
            {
                var unmatched = new HashSet<ParameterInfo>(constructor.GetParameters());

                foreach (var field in schema.Fields)
                {
                    var match = unmatched
                        .FirstOrDefault(parameter => IsMatch(field, parameter.Name));

                    if (match == null)
                    {
                        return false;
                    }

                    unmatched.Remove(match);
                }

                if (unmatched.Any(parameter => !parameter.HasDefaultValue))
                {
                    return false;
                }

                return true;
            });
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
