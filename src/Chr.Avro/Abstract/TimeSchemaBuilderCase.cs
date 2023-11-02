#if NET6_0_OR_GREATER
namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a schema builder case that matches <see cref="TimeOnly" />.
    /// </summary>
    public class TimeSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TimeSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="temporalBehavior">
        /// A value indicating whether the case should build string schemas (ISO 8601) or int/long
        /// schemas (time logical types).
        /// </param>
        public TimeSchemaBuilderCase(TemporalBehavior temporalBehavior)
        {
            TemporalBehavior = temporalBehavior;
        }

        /// <summary>
        /// Gets a value indicating whether the case should build string schemas (ISO 8601) or
        /// int/long schemas (time logical types).
        /// </summary>
        public TemporalBehavior TemporalBehavior { get; }

        /// <summary>
        /// Builds an <see cref="IntSchema" /> or <see cref="LongSchema" /> with a <see cref="TimeLogicalType" />
        /// or a <see cref="StringSchema" /> based on the value of <see cref="TemporalBehavior" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="IntSchema" />
        /// or <see cref="LongSchema" /> and <see cref="TimeLogicalType" /> or <see cref="StringSchema" />
        /// if <paramref name="type" /> is <see cref="TimeOnly" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(TimeOnly))
            {
                Schema timeSchema = TemporalBehavior switch
                {
                    TemporalBehavior.EpochMicroseconds => new LongSchema()
                    {
                        LogicalType = new MicrosecondTimeLogicalType(),
                    },
                    TemporalBehavior.EpochMilliseconds => new IntSchema()
                    {
                        LogicalType = new MillisecondTimeLogicalType(),
                    },
                    TemporalBehavior.Iso8601 => new StringSchema(),
                    _ => throw new ArgumentOutOfRangeException(nameof(TemporalBehavior)),
                };

                try
                {
                    context.Schemas.Add(type, timeSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(timeSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(TimeSchemaBuilderCase)} can only be applied to the {nameof(TimeOnly)} type."));
            }
        }
    }
}
#endif
