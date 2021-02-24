namespace Chr.Avro.Representation
{
    using System;
    using System.Collections.Generic;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Represents the state of a <see cref="JsonSchemaReader" /> operation.
    /// </summary>
    public class JsonSchemaReaderContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSchemaReaderContext" /> class.
        /// </summary>
        public JsonSchemaReaderContext()
        {
            Schemas = new Dictionary<string, Schema>();
        }

        /// <summary>
        /// Gets a map of identifiers to <see cref="Schema" />s. If a <see cref="Schema" /> is
        /// present for a specific identifier, that <see cref="Schema" /> will be returned by the
        /// <see cref="JsonSchemaReader" /> for all subsequent occurrences of the <see cref="Type" />.
        /// This ensures that the identities of schemas are consistent (i.e., the same abstract
        /// instance will represent a schema wherever it appears) and enables recursive operations,
        /// like building a <see cref="RecordSchema" />. <see cref="Schema" />s in this collection
        /// shouldn’t be assumed to be complete until the top-level read operation has completed.
        /// </summary>
        public virtual IDictionary<string, Schema> Schemas { get; }

        /// <summary>
        /// Gets or sets the surrounding namespace, if any.
        /// </summary>
        public virtual string? Scope { get; set; }
    }
}
