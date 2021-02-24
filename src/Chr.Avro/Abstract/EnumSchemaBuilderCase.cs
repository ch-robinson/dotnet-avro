namespace Chr.Avro.Abstract
{
    using System;
    using System.Linq;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="EnumResolution" />.
    /// </summary>
    public class EnumSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="EnumSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve underlying integral types.
        /// </param>
        public EnumSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(SchemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Gets the schema builder instance that will be used to resolve underlying integral types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="EnumSchema" />
        /// if <paramref name="resolution" /> is an <see cref="EnumResolution" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is EnumResolution enumResolution)
            {
                var enumSchema = enumResolution.IsFlagEnum switch
                {
                    true => SchemaBuilder.BuildSchema(enumResolution.UnderlyingType, context),
                    false => new EnumSchema(
                        enumResolution.Namespace == null
                            ? enumResolution.Name.Value
                            : $"{enumResolution.Namespace.Value}.{enumResolution.Name.Value}",
                        enumResolution.Symbols.Select(symbol => symbol.Name.Value))
                };

                try
                {
                    context.Schemas.Add(enumResolution.Type.GetUnderlyingType(), enumSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {enumResolution.Type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(enumSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(EnumSchemaBuilderCase)} can only be applied to {nameof(EnumResolution)}s."));
            }
        }
    }
}
