namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a schema builder case that matches <see cref="DateTime" /> and
    /// <see cref="DateTimeOffset" />.
    /// </summary>
    public class TimestampSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TimestampSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="temporalBehavior">
        /// A value indicating whether the case should build string schemas (ISO 8601) or long
        /// schemas (timestamp logical types).
        /// </param>
        public TimestampSchemaBuilderCase(TemporalBehavior temporalBehavior)
        {
            TemporalBehavior = temporalBehavior;
        }

        /// <summary>
        /// Gets a value indicating whether the case should build string schemas (ISO 8601) or long
        /// schemas (timestamp logical types).
        /// </summary>
        public TemporalBehavior TemporalBehavior { get; }

        /// <summary>
        /// Builds a <see cref="LongSchema" /> with a <see cref="TimestampLogicalType" /> or a
        /// <see cref="StringSchema" /> based on the value of <see cref="TemporalBehavior" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="LongSchema" />
        /// and <see cref="TimestampLogicalType" /> or <see cref="StringSchema" /> if
        /// <paramref name="type" /> is <see cref="DateTime" /> or <see cref="DateTimeOffset" />;
        /// an unsuccessful <see cref="SchemaBuilderCaseResult" /> with an
        /// <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
            {
                Schema timestampSchema = TemporalBehavior switch
                {
                    TemporalBehavior.EpochMicroseconds => new LongSchema()
                    {
                        LogicalType = new MicrosecondTimestampLogicalType(),
                    },
                    TemporalBehavior.EpochMilliseconds => new LongSchema()
                    {
                        LogicalType = new MillisecondTimestampLogicalType(),
                    },
                    TemporalBehavior.EpochNanoseconds => new LongSchema()
                    {
                        LogicalType = new NanosecondTimestampLogicalType(),
                    },
                    TemporalBehavior.Iso8601 => new StringSchema(),
                    _ => throw new ArgumentOutOfRangeException(nameof(TemporalBehavior)),
                };

                try
                {
                    context.Schemas.Add(type, timestampSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(timestampSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(TimestampSchemaBuilderCase)} can only be applied to the {nameof(DateTime)} and {nameof(DateTimeOffset)} types."));
            }
        }
    }
}
