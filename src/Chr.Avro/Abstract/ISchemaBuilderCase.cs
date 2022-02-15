namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Defines methods to build Avro schemas for specific <see cref="Type" />s.
    /// </summary>
    public interface ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a schema for a <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// The <see cref="Type" /> to build a <see cref="Schema" /> for.
        /// </param>
        /// <param name="context">
        /// A <see cref="SchemaBuilderContext" /> representing the state of the build operation.
        /// </param>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> if the case can be applied; an
        /// unsuccessful <see cref="SchemaBuilderCaseResult" /> otherwise.
        /// </returns>
        SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context);
    }
}
