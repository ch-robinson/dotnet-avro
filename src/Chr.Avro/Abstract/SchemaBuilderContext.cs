namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represents the state of a <see cref="SchemaBuilder" /> operation.
    /// </summary>
    public class SchemaBuilderContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaBuilderContext" /> class.
        /// </summary>
        public SchemaBuilderContext()
        {
            Schemas = new Dictionary<Type, Schema>();
        }

        /// <summary>
        /// Gets a map of <see cref="Type" />s to <see cref="Schema" />s. If a
        /// <see cref="Schema" /> is present for a specific <see cref="Type" />, that
        /// <see cref="Schema" /> will be returned by the <see cref="SchemaBuilder" /> for all
        /// subsequent occurrences of the <see cref="Type" />. This is necessary for potentially
        /// recursive operations such as building <see cref="RecordSchema" />s. <see cref="Schema" />s
        /// in this collection shouldn’t be assumed to be complete until the top-level build
        /// operation has completed.
        /// </summary>
        public virtual IDictionary<Type, Schema> Schemas { get; }
    }
}
