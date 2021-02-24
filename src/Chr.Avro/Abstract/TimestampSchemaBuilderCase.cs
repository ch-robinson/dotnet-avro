namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a schema builder case that matches <see cref="TimestampResolution" />.
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
        /// <paramref name="resolution" /> is an <see cref="TimestampResolution" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is TimestampResolution timestampResolution)
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
                    TemporalBehavior.Iso8601 => new StringSchema(),
                    _ => throw new ArgumentOutOfRangeException(nameof(TemporalBehavior))
                };

                try
                {
                    context.Schemas.Add(timestampResolution.Type.GetUnderlyingType(), timestampSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {timestampResolution.Type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(timestampSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(TimestampSchemaBuilderCase)} can only be applied to {nameof(TimestampResolution)}s."));
            }
        }
    }
}
