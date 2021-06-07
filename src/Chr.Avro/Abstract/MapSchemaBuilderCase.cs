namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches
    /// <see cref="IDictionary{TKey, TValue}" /> types.
    /// </summary>
    public class MapSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MapSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="nullableReferenceTypeBehavior">
        /// The behavior to use to determine nullability of reference types.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to build schemas for value types.
        /// </param>
        public MapSchemaBuilderCase(
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
        /// Gets the schema builder instance that will be used to build schemas for value types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds a <see cref="MapSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="MapSchema" /> if
        /// <paramref name="type" /> implements <see cref="IEnumerable{T}" /> and the item type is
        /// <see cref="KeyValuePair{TKey, TValue}" />; an unsuccessful <see cref="SchemaBuilderCaseResult" />
        /// with an <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type.GetDictionaryTypes()?.Value is Type valueType)
            {
                // defer setting the value schema so the map schema can be cached:
                var mapSchema = ReflectionExtensions.GetUninitializedInstance<MapSchema>();

                Schema schema = mapSchema;

                if (!type.IsValueType && NullableReferenceTypeBehavior == NullableReferenceTypeBehavior.All)
                {
                    schema = new UnionSchema(new[] { new NullSchema(), schema });
                }

                try
                {
                    context.Schemas.Add(type, schema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                mapSchema.Value = SchemaBuilder.BuildSchema(valueType, context);

                return SchemaBuilderCaseResult.FromSchema(schema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(MapSchemaBuilderCase)} can only be applied to dictionary types."));
            }
        }
    }
}
