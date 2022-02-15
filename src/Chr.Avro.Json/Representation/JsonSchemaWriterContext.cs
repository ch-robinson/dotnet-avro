namespace Chr.Avro.Representation
{
    using System.Collections.Generic;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Represents the state of a <see cref="JsonSchemaWriter" /> operation.
    /// </summary>
    public class JsonSchemaWriterContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSchemaWriterContext" /> class.
        /// </summary>
        public JsonSchemaWriterContext()
        {
            Names = new Dictionary<string, NamedSchema>();
        }

        /// <summary>
        /// Gets a map of names to <see cref="NamedSchema" />s. the <see cref="JsonSchemaWriter" />
        /// uses this collection to determine whether to write out a schema or to only write out
        /// its name.
        /// </summary>
        public virtual IDictionary<string, NamedSchema> Names { get; }
    }
}
