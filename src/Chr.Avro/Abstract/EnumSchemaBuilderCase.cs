namespace Chr.Avro.Abstract
{
    using System;
    using System.Linq;
    using System.Reflection;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="Enum" /> types.
    /// </summary>
    public class EnumSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="EnumSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="enumBehavior">
        /// A value value indicating whether the case should build enum schemas or integral schemas.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to build schemas for underlying integral
        /// types.
        /// </param>
        public EnumSchemaBuilderCase(EnumBehavior enumBehavior, ISchemaBuilder schemaBuilder)
        {
            EnumBehavior = enumBehavior;
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(SchemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Gets a value indicating whether the case should build enum schemas or integral schemas.
        /// </summary>
        public EnumBehavior EnumBehavior { get; }

        /// <summary>
        /// Gets the schema builder instance that will be used to build schemas for underlying
        /// integral types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="EnumSchema" />
        /// if <paramref name="type" /> is an <see cref="Enum" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type.IsEnum)
            {
                Schema schema;

                if (type.GetAttribute<FlagsAttribute>() == null && EnumBehavior == EnumBehavior.Symbolic)
                {
                    schema = new EnumSchema(type.Name)
                    {
                        Namespace = string.IsNullOrEmpty(type.Namespace) ? null : type.Namespace,

                        // enum fields will always be public static, so no need to expose binding flags:
                        Symbols = type.GetFields(BindingFlags.Public | BindingFlags.Static)
                            .OrderBy(field => Enum.Parse(type, field.Name))
                            .ThenBy(field => field.Name)
                            .Select(field => field.Name)
                            .ToList(),
                    };
                }
                else
                {
                    schema = SchemaBuilder.BuildSchema(type.GetEnumUnderlyingType(), context);
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
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(EnumSchemaBuilderCase)} can only be applied to {typeof(Enum)} types."));
            }
        }
    }
}
