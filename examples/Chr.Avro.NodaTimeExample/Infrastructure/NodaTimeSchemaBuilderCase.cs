namespace Chr.Avro.NodaTimeExample.Infrastructure
{
    using System;
    using Chr.Avro.Abstract;

    public class NodaTimeSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        public NodaTimeSchemaBuilderCase(TemporalBehavior temporalBehavior)
        {
            TemporalBehavior = temporalBehavior;
        }

        public TemporalBehavior TemporalBehavior { get; }

        public SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            // Handle NodaTime.Instant like the TimestampSchemaBuilderCase
            if (type == typeof(NodaTime.Instant))
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
                    _ => throw new ArgumentOutOfRangeException(nameof(TemporalBehavior)),
                };

                try
                {
                    // Important: Add NodaTime-type here, not DateTime or DateTimeOffset.
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
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(TimestampSchemaBuilderCase)} can only be applied to the {nameof(NodaTime.Instant)} type."));
            }
        }
    }
}
