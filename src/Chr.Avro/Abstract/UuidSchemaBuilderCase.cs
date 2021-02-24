namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="UuidResolution" />.
    /// </summary>
    public class UuidSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="StringSchema" /> with a <see cref="UuidLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="StringSchema" />
        /// and associated <see cref="UuidLogicalType" /> if <paramref name="resolution" /> is a
        /// <see cref="UuidResolution" />; an unsuccessful <see cref="SchemaBuilderCaseResult" />
        /// with an <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is UuidResolution uuidResolution)
            {
                var uuidSchema = new StringSchema()
                {
                    LogicalType = new UuidLogicalType(),
                };

                try
                {
                    context.Schemas.Add(uuidResolution.Type.GetUnderlyingType(), uuidSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {uuidResolution.Type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(uuidSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(UuidSchemaBuilderCase)} can only be applied to {nameof(UuidResolution)}s."));
            }
        }
    }
}
