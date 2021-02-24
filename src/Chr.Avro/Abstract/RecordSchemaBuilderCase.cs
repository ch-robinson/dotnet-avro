namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="RecordResolution" />.
    /// </summary>
    public class RecordSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RecordSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve record field types.
        /// </param>
        public RecordSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Gets the schema builder instance that will be used to resolve record field types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="RecordSchema" />
        /// if <paramref name="resolution" /> is a <see cref="RecordResolution" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            var result = new SchemaBuilderCaseResult();

            if (resolution is RecordResolution recordResolution)
            {
                // defer setting the field schemas so the record schema can be cached:
                var recordSchema = new RecordSchema(
                    recordResolution.Namespace == null
                        ? recordResolution.Name.Value
                        : $"{recordResolution.Namespace.Value}.{recordResolution.Name.Value}");

                try
                {
                    context.Schemas.Add(recordResolution.Type.GetUnderlyingType(), recordSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {recordResolution.Type} already exists on the schema builder context.", exception);
                }

                foreach (var field in recordResolution.Fields)
                {
                    try
                    {
                        recordSchema.Fields.Add(new RecordField(field.Name.Value, SchemaBuilder.BuildSchema(field.Type, context)));
                    }
                    catch (Exception exception)
                    {
                        throw new UnsupportedTypeException(recordResolution.Type, $"A schema could not be built for the {field.Name} field on {recordResolution.Type}.", exception);
                    }
                }

                return SchemaBuilderCaseResult.FromSchema(recordSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(RecordSchemaBuilderCase)} can only be applied to {nameof(RecordResolution)}s."));
            }
        }
    }
}
