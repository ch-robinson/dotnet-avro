namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="T:System.Nullable`1" />.
    /// </summary>
    public class UnionSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UnionSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to build schemas for union member types.
        /// </param>
        public UnionSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Gets the schema builder instance that will be used to build schemas for union member
        /// types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds a <see cref="UnionSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="UnionSchema" />
        /// if <paramref name="type" /> is <see cref="T:System.Nullable`1" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (Nullable.GetUnderlyingType(type) is Type underlyingType)
            {
                // defer setting the item schema so the union schema can be cached:
                var unionSchema = new UnionSchema();

                try
                {
                    context.Schemas.Add(type, unionSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                unionSchema.Schemas.Add(new NullSchema());
                unionSchema.Schemas.Add(SchemaBuilder.BuildSchema(underlyingType, context));

                return SchemaBuilderCaseResult.FromSchema(unionSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(UnionSchemaBuilderCase)} can only be applied to nullable value types."));
            }
        }
    }
}
