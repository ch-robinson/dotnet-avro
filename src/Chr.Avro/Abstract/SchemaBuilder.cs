namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Builds Avro schemas for .NET <see cref="Type" />s.
    /// </summary>
    public class SchemaBuilder : ISchemaBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaBuilder" /> class configured with
        /// the default list of cases.
        /// </summary>
        /// <param name="temporalBehavior">
        /// Whether the builder should build string schemas (ISO 8601) or long schemas (timestamp
        /// logical types) for timestamp resolutions.
        /// </param>
        /// <param name="typeResolver">
        /// The <see cref="ITypeResolver" /> that should be used to retrieve type information. If
        /// no <see cref="ITypeResolver" /> is provided, the <see cref="SchemaBuilder" /> will use
        /// a <see cref="TypeResolver" /> with the default set of cases.
        /// </param>
        public SchemaBuilder(
            TemporalBehavior temporalBehavior = TemporalBehavior.Iso8601,
            ITypeResolver? typeResolver = null)
            : this(CreateDefaultCaseBuilders(temporalBehavior), typeResolver)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaBuilder" /> class configured with a
        /// custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="typeResolver">
        /// The <see cref="ITypeResolver" /> that should be used to retrieve type information. If
        /// no <see cref="ITypeResolver" /> is provided, the <see cref="SchemaBuilder" /> will use
        /// a <see cref="TypeResolver" /> with the default set of cases.
        /// </param>
        public SchemaBuilder(
            IEnumerable<Func<ISchemaBuilder, ISchemaBuilderCase>> caseBuilders,
            ITypeResolver? typeResolver = null)
        {
            var cases = new List<ISchemaBuilderCase>();

            Cases = cases;
            Resolver = typeResolver ?? new TypeResolver();

            // initialize cases last so that the schema builder is fully ready:
            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
        }

        /// <summary>
        /// Gets the list of cases that the schema builder will attempt to apply. If the first case
        /// does not match, the schema builder will try the next case, and so on until all cases
        /// have been tried.
        /// </summary>
        public virtual IEnumerable<ISchemaBuilderCase> Cases { get; }

        /// <summary>
        /// Gets the resolver that will be used to retrieve type information.
        /// </summary>
        public virtual ITypeResolver Resolver { get; }

        /// <summary>
        /// Creates the default list of case builders.
        /// </summary>
        /// <param name="temporalBehavior">
        /// The behavior to apply when building schemas for temporal types.
        /// </param>
        /// <returns>
        /// A list of case builders that matches most .NET <see cref="Type" />s.
        /// </returns>
        public static IEnumerable<Func<ISchemaBuilder, ISchemaBuilderCase>> CreateDefaultCaseBuilders(TemporalBehavior temporalBehavior)
        {
            return new Func<ISchemaBuilder, ISchemaBuilderCase>[]
            {
                builder => new ArraySchemaBuilderCase(builder),
                builder => new BooleanSchemaBuilderCase(),
                builder => new BytesSchemaBuilderCase(),
                builder => new DecimalSchemaBuilderCase(),
                builder => new DoubleSchemaBuilderCase(),
                builder => new DurationSchemaBuilderCase(),
                builder => new EnumSchemaBuilderCase(builder),
                builder => new FloatSchemaBuilderCase(),
                builder => new IntSchemaBuilderCase(),
                builder => new LongSchemaBuilderCase(),
                builder => new MapSchemaBuilderCase(builder),
                builder => new RecordSchemaBuilderCase(builder),
                builder => new StringSchemaBuilderCase(),
                builder => new TimestampSchemaBuilderCase(temporalBehavior),
                builder => new UriSchemaBuilderCase(),
                builder => new UuidSchemaBuilderCase(),
            };
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case matches <typeparamref name="T" /> or when a matching case fails.
        /// </exception>
        /// <inheritdoc />
        public virtual Schema BuildSchema<T>(SchemaBuilderContext? context = null)
        {
            return BuildSchema(typeof(T), context);
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case matches <paramref name="type" /> or when a matching case fails.
        /// </exception>
        /// <inheritdoc />
        public virtual Schema BuildSchema(Type type, SchemaBuilderContext? context = null)
        {
            context ??= new SchemaBuilderContext();

            var resolution = Resolver.ResolveType(type);

            if (!context.Schemas.TryGetValue(resolution.Type, out var schema))
            {
                // first try to build a schema for the underlying type, if any:
                if (!context.Schemas.TryGetValue(resolution.Type.GetUnderlyingType(), out schema))
                {
                    var exceptions = new List<Exception>();

                    foreach (var @case in Cases)
                    {
                        var result = @case.BuildSchema(resolution, context);

                        if (result.Schema != null)
                        {
                            schema = result.Schema;
                            break;
                        }

                        exceptions.AddRange(result.Exceptions);
                    }

                    if (schema == null)
                    {
                        throw new UnsupportedTypeException(resolution.Type, $"No schema builder case could be applied to {resolution.Type} (as {resolution.GetType().Name}).", new AggregateException(exceptions));
                    }
                }

                // then, if nullable, ensure the union is cached:
                if (resolution.IsNullable)
                {
                    context.Schemas[resolution.Type] = schema = new UnionSchema(new Schema[] { new NullSchema(), schema });
                }
            }

            return schema;
        }
    }
}
