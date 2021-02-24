namespace Chr.Avro.Representation
{
    /// <summary>
    /// Provides a base schema reader case implementation.
    /// </summary>
    public abstract class SchemaReaderCase
    {
        /// <summary>
        /// Qualifies a schema name.
        /// </summary>
        /// <param name="name">
        /// A value assumed to be a legal Avro name (qualified or unqualified).
        /// </param>
        /// <param name="scope">
        /// A value assumed to be a legal Avro namespace.
        /// </param>
        /// <returns>
        /// <paramref name="scope" />.<paramref name="name" /> if <paramref name="scope" /> is not
        /// <c>null</c> and <paramref name="name" /> is unqualified; <paramref name="name" />
        /// otherwise.
        /// </returns>
        protected virtual string QualifyName(string name, string? scope)
        {
            return name.Contains(".") == false && scope != null
                ? $"{scope}.{name}"
                : name;
        }
    }
}
