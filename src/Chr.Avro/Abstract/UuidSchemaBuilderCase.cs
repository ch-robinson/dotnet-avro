namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="Guid" />.
    /// </summary>
    public class UuidSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="StringSchema" /> with a <see cref="UuidLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="StringSchema" />
        /// and associated <see cref="UuidLogicalType" /> if <paramref name="type" /> is
        /// <see cref="Guid" />; an unsuccessful <see cref="SchemaBuilderCaseResult" />
        /// with an <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(Guid))
            {
                var uuidSchema = new StringSchema()
                {
                    LogicalType = new UuidLogicalType(),
                };

                try
                {
                    context.Schemas.Add(type, uuidSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(uuidSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(UuidSchemaBuilderCase)} can only be applied to the {nameof(Guid)} type."));
            }
        }
    }
}
