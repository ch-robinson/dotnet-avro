namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="ArrayResolution" />.
    /// </summary>
    public class ArraySchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ArraySchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve array item types.
        /// </param>
        public ArraySchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Gets the schema builder instance that will be used to resolve array item types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds an <see cref="ArraySchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="ArraySchema" />
        /// if <paramref name="resolution" /> is an <see cref="ArrayResolution" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is ArrayResolution arrayResolution)
            {
                // defer setting the item schema so the array schema can be cached:
                var arraySchema = ReflectionExtensions.GetUninitializedInstance<ArraySchema>();

                try
                {
                    context.Schemas.Add(arrayResolution.Type.GetUnderlyingType(), arraySchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {arrayResolution.Type} already exists on the schema builder context.", exception);
                }

                arraySchema.Item = SchemaBuilder.BuildSchema(arrayResolution.ItemType, context);

                return SchemaBuilderCaseResult.FromSchema(arraySchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(ArraySchemaBuilderCase)} can only be applied to {nameof(ArrayResolution)}s."));
            }
        }
    }
}
