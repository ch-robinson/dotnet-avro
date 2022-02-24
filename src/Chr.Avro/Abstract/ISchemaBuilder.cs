namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Defines methods to build Avro schemas for .NET <see cref="Type" />s.
    /// </summary>
    public interface ISchemaBuilder
    {
        /// <summary>
        /// Builds a schema.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> to build a schema for.
        /// </typeparam>
        /// <param name="context">
        /// An optional schema builder context. A context can be provided to predefine schemas for
        /// certain <see cref="Type" />s or to grant the caller access to inner results; if no
        /// context is provided, an empty context will be created.
        /// </param>
        /// <returns>
        /// A schema that matches <typeparamref name="T" />.
        /// </returns>
        Schema BuildSchema<T>(SchemaBuilderContext? context = default);

        /// <summary>
        /// Builds a schema.
        /// </summary>
        /// <param name="type">
        /// The <see cref="Type" /> to build a schema for.
        /// </param>
        /// <param name="context">
        /// An optional schema builder context. A context can be provided to predefine schemas for
        /// certain <see cref="Type" />s or to grant the caller access to inner results; if no
        /// context is provided, an empty context will be created.
        /// </param>
        /// <returns>
        /// A schema that matches <paramref name="type" />.
        /// </returns>
        Schema BuildSchema(Type type, SchemaBuilderContext? context = default);
    }
}
