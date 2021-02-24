namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Defines methods to build Avro schemas for specific <see cref="TypeResolution" />s.
    /// </summary>
    public interface ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a schema for a <see cref="TypeResolution" />.
        /// </summary>
        /// <param name="resolution">
        /// A <see cref="TypeResolution" /> to extract <see cref="Type" /> information from.
        /// </param>
        /// <param name="context">
        /// A <see cref="SchemaBuilderContext" /> representing the state of the build operation.
        /// </param>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> if the case can be applied; an
        /// unsuccessful <see cref="SchemaBuilderCaseResult" /> otherwise.
        /// </returns>
        SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context);
    }
}
