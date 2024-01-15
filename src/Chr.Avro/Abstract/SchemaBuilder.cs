namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Builds Avro schemas for .NET <see cref="Type" />s.
    /// </summary>
    public class SchemaBuilder : ISchemaBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaBuilder" /> class configured with
        /// the default list of cases.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags the builder should use to select fields and properties.
        /// </param>
        /// <param name="enumBehavior">
        /// Whether the builder should build enum schemas or integral schemas for enum types.
        /// </param>
        /// <param name="nullableReferenceTypeBehavior">
        /// The behavior the builder should apply when determining nullability of reference types.
        /// </param>
        /// <param name="temporalBehavior">
        /// Whether the builder should build string schemas (ISO 8601) or long schemas (timestamp
        /// logical types) for timestamp types.
        /// </param>
        public SchemaBuilder(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance,
            EnumBehavior enumBehavior = EnumBehavior.Symbolic,
            NullableReferenceTypeBehavior nullableReferenceTypeBehavior = NullableReferenceTypeBehavior.Annotated,
            TemporalBehavior temporalBehavior = TemporalBehavior.Iso8601)
            : this(CreateDefaultCaseBuilders(memberVisibility, enumBehavior, nullableReferenceTypeBehavior, temporalBehavior))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaBuilder" /> class configured with a
        /// custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        public SchemaBuilder(
            IEnumerable<Func<ISchemaBuilder, ISchemaBuilderCase>> caseBuilders)
        {
            var cases = new List<ISchemaBuilderCase>();

            Cases = cases;

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
        /// Creates the default list of case builders.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <param name="enumBehavior">
        /// The behavior to apply when building schemas for enum types.
        /// </param>
        /// <param name="nullableReferenceTypeBehavior">
        /// The behavior to apply when determining nullability of reference types.
        /// </param>
        /// <param name="temporalBehavior">
        /// The behavior to apply when building schemas for temporal types.
        /// </param>
        /// <returns>
        /// A list of case builders that matches most .NET <see cref="Type" />s.
        /// </returns>
        public static IEnumerable<Func<ISchemaBuilder, ISchemaBuilderCase>> CreateDefaultCaseBuilders(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance,
            EnumBehavior enumBehavior = EnumBehavior.Symbolic,
            NullableReferenceTypeBehavior nullableReferenceTypeBehavior = NullableReferenceTypeBehavior.None,
            TemporalBehavior temporalBehavior = TemporalBehavior.Iso8601)
        {
            return new Func<ISchemaBuilder, ISchemaBuilderCase>[]
            {
                // nullables:
                builder => new UnionSchemaBuilderCase(builder),

                // primitives:
                builder => new BooleanSchemaBuilderCase(),
                builder => new BytesSchemaBuilderCase(nullableReferenceTypeBehavior),
                builder => new DecimalSchemaBuilderCase(),
                builder => new DoubleSchemaBuilderCase(),
                builder => new FloatSchemaBuilderCase(),
                builder => new IntSchemaBuilderCase(),
                builder => new LongSchemaBuilderCase(),
                builder => new StringSchemaBuilderCase(nullableReferenceTypeBehavior),

                // enums:
                builder => new EnumSchemaBuilderCase(enumBehavior, builder),

                // dictionaries:
                builder => new MapSchemaBuilderCase(nullableReferenceTypeBehavior, builder),

                // enumerables:
                builder => new ArraySchemaBuilderCase(nullableReferenceTypeBehavior, builder),

                // built-ins:
#if NET6_0_OR_GREATER
                builder => new DateSchemaBuilderCase(temporalBehavior),
#endif
                builder => new DurationSchemaBuilderCase(),
#if NET6_0_OR_GREATER
                builder => new TimeSchemaBuilderCase(temporalBehavior),
#endif
                builder => new TimestampSchemaBuilderCase(temporalBehavior),
                builder => new UriSchemaBuilderCase(nullableReferenceTypeBehavior),
                builder => new UuidSchemaBuilderCase(),

                // classes and structs:
                builder => new RecordSchemaBuilderCase(memberVisibility, nullableReferenceTypeBehavior, builder),
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

            if (!context.Schemas.TryGetValue(type, out var schema))
            {
                var exceptions = new List<Exception>();

                foreach (var @case in Cases)
                {
                    var result = @case.BuildSchema(type, context);

                    if (result.Schema != null)
                    {
                        schema = result.Schema;
                        break;
                    }

                    exceptions.AddRange(result.Exceptions);
                }

                if (schema == null)
                {
                    throw new UnsupportedTypeException(type, $"No schema builder case could be applied to {type}.", new AggregateException(exceptions));
                }
            }

            return schema;
        }
    }
}
