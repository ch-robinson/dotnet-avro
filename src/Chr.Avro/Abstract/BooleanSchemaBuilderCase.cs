namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="BooleanResolution" />.
    /// </summary>
    public class BooleanSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BooleanSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="BooleanSchema" />
        /// if <paramref name="resolution" /> is a <see cref="BooleanResolution" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is BooleanResolution booleanResolution)
            {
                var booleanSchema = new BooleanSchema();

                try
                {
                    context.Schemas.Add(booleanResolution.Type.GetUnderlyingType(), booleanSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {booleanResolution.Type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(booleanSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(BooleanSchemaBuilderCase)} can only be applied to {nameof(BooleanResolution)}s."));
            }
        }
    }
}
