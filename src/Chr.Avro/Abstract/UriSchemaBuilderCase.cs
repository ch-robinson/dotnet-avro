namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="UriResolution" />.
    /// </summary>
    public class UriSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="StringSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="StringSchema" />
        /// if <paramref name="resolution" /> is a <see cref="UriResolution" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is UriResolution uriResolution)
            {
                var uriSchema = new StringSchema();

                try
                {
                    context.Schemas.Add(uriResolution.Type.GetUnderlyingType(), uriSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {uriResolution.Type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(uriSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(UriSchemaBuilderCase)} can only be applied to {nameof(UriResolution)}s."));
            }
        }
    }
}
