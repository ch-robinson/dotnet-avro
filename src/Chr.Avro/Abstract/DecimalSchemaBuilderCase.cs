namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="DecimalResolution" />.
    /// </summary>
    public class DecimalSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BytesSchema" /> with a <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="BytesSchema" />
        /// and associated <see cref="DecimalLogicalType" /> if <paramref name="resolution" /> is a
        /// <see cref="DecimalResolution" />; an unsuccessful <see cref="SchemaBuilderCaseResult" />
        /// with an <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is DecimalResolution decimalResolution)
            {
                var decimalSchema = new BytesSchema()
                {
                    LogicalType = new DecimalLogicalType(decimalResolution.Precision, decimalResolution.Scale),
                };

                try
                {
                    context.Schemas.Add(decimalResolution.Type.GetUnderlyingType(), decimalSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {decimalResolution.Type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(decimalSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(DecimalSchemaBuilderCase)} can only be applied to {nameof(DecimalResolution)}s."));
            }
        }
    }
}
