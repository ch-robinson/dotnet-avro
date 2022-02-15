namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="T:System.Byte[]" />.
    /// </summary>
    public class BytesSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BytesSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="nullableReferenceTypeBehavior">
        /// The behavior to use to determine nullability of reference types.
        /// </param>
        public BytesSchemaBuilderCase(NullableReferenceTypeBehavior nullableReferenceTypeBehavior)
        {
            NullableReferenceTypeBehavior = nullableReferenceTypeBehavior;
        }

        /// <summary>
        /// Gets the behavior used to determine nullability of reference types.
        /// </summary>
        public NullableReferenceTypeBehavior NullableReferenceTypeBehavior { get; }

        /// <summary>
        /// Builds a <see cref="BytesSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="BytesSchema" />
        /// if <paramref name="type" /> is <see cref="T:System.Byte[]" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(byte[]))
            {
                var bytesSchema = new BytesSchema();

                Schema schema = bytesSchema;

                if (!type.IsValueType && NullableReferenceTypeBehavior == NullableReferenceTypeBehavior.All)
                {
                    if (!context.Schemas.TryGetValue(NullableType, out var nullSchema))
                    {
                        context.Schemas.Add(NullableType, nullSchema = new NullSchema());
                    }

                    schema = new UnionSchema(new[] { nullSchema, schema });
                }

                try
                {
                    context.Schemas.Add(type, schema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(schema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BytesSchemaBuilderCase)} can only be applied to the {typeof(byte[])} type."));
            }
        }
    }
}
