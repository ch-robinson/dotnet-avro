namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="StringResolution" />.
    /// </summary>
    public class StringSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="StringSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="StringSchema" />
        /// if <paramref name="resolution" /> is a <see cref="StringResolution" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is StringResolution stringResolution)
            {
                var stringSchema = new StringSchema();

                try
                {
                    context.Schemas.Add(stringResolution.Type.GetUnderlyingType(), stringSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {stringResolution.Type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(stringSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(StringSchemaBuilderCase)} can only be applied to {nameof(StringResolution)}s."));
            }
        }
    }
}
