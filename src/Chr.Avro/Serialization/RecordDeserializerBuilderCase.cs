namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Text.RegularExpressions;
    using Chr.Avro.Abstract;

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
                schema.Fields.All(field =>
                    constructor.GetParameters().Any(parameter =>
                        IsMatch(field, parameter.Name))));
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
