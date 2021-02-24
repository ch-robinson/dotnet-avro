namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="MapResolution" />.
    /// </summary>
    public class MapSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MapSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve map value types.
        /// </param>
        public MapSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Gets the schema builder instance that will be used to resolve map value types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds a <see cref="MapSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="MapSchema" /> if
        /// <paramref name="resolution" /> is a <see cref="MapResolution" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is MapResolution mapResolution)
            {
                // defer setting the value schema so the map schema can be cached:
                var mapSchema = ReflectionExtensions.GetUninitializedInstance<MapSchema>();

                try
                {
                    context.Schemas.Add(mapResolution.Type.GetUnderlyingType(), mapSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {mapResolution.Type} already exists on the schema builder context.", exception);
                }

                mapSchema.Value = SchemaBuilder.BuildSchema(mapResolution.ValueType, context);

                return SchemaBuilderCaseResult.FromSchema(mapSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(MapSchemaBuilderCase)} can only be applied to {nameof(MapResolution)}s."));
            }
        }
    }
}
