namespace Chr.Avro.Serialization
{
    using System;
    using System.Text.RegularExpressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="RecordSchema" />.
    /// </summary>
    public abstract class RecordSerializerBuilderCase : SerializerBuilderCase
    {
        private static readonly Regex FuzzyCharacters = new(@"[^A-Za-z0-9]");

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
