namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="IEnumerable{T}" />
    /// types.
    /// </summary>
    public class ArraySchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ArraySchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="nullableReferenceTypeBehavior">
        /// The behavior to use to determine nullability of reference types.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to build item schemas.
        /// </param>
        public ArraySchemaBuilderCase(
            NullableReferenceTypeBehavior nullableReferenceTypeBehavior,
            ISchemaBuilder schemaBuilder)
        {
            NullableReferenceTypeBehavior = nullableReferenceTypeBehavior;
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Gets the behavior used to determine nullability of reference types.
        /// </summary>
        public NullableReferenceTypeBehavior NullableReferenceTypeBehavior { get; }

        /// <summary>
        /// Gets the schema builder instance that will be used to build item schemas.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds an <see cref="ArraySchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="ArraySchema" />
        /// if <paramref name="type" /> implements <see cref="IEnumerable{T}" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (GetEnumerableType(type) is Type itemType)
            {
                // defer setting the item schema so the array schema can be cached:
                var arraySchema = ReflectionExtensions.GetUninitializedInstance<ArraySchema>();

                Schema schema = arraySchema;

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

                arraySchema.Item = SchemaBuilder.BuildSchema(itemType, context);

                return SchemaBuilderCaseResult.FromSchema(schema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(ArraySchemaBuilderCase)} can only be applied to enumerable types."));
            }
        }

        /// <summary>
        /// Gets the item <see cref="Type" /> of an enumerable <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> object that describes a generic enumerable.
        /// </param>
        /// <returns>
        /// If <paramref name="type" /> implements (or is) <see cref="IEnumerable{T}" />, its type
        /// argument; <c>null</c> otherwise.
        /// </returns>
        protected virtual Type? GetEnumerableType(Type type)
        {
            return type.GetEnumerableType();
        }
    }
}
