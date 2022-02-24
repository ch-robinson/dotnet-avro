namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="decimal" />.
    /// </summary>
    public class DecimalSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BytesSchema" /> with a <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="BytesSchema" />
        /// and associated <see cref="DecimalLogicalType" /> if <paramref name="type" /> is
        /// <see cref="decimal" />; an unsuccessful <see cref="SchemaBuilderCaseResult" /> with an
        /// <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(decimal))
            {
                var decimalSchema = new BytesSchema
                {
                    // default precision/scale to .NET limits:
                    LogicalType = new DecimalLogicalType(29, 14),
                };

                try
                {
                    context.Schemas.Add(type, decimalSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(decimalSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(DecimalSchemaBuilderCase)} can only be applied to the {typeof(decimal)} type."));
            }
        }
    }
}
