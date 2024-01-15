#if NET6_0_OR_GREATER
namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a schema builder case that matches <see cref="DateOnly" />.
    /// </summary>
    public class DateSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DateSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="temporalBehavior">
        /// A value indicating whether the case should build string schemas (ISO 8601) or int
        /// schemas (date logical type).
        /// </param>
        public DateSchemaBuilderCase(TemporalBehavior temporalBehavior)
        {
            TemporalBehavior = temporalBehavior;
        }

        /// <summary>
        /// Gets a value indicating whether the case should build string schemas (ISO 8601) or int
        /// schemas (date logical type).
        /// </summary>
        public TemporalBehavior TemporalBehavior { get; }

        /// <summary>
        /// Builds an <see cref="IntSchema" /> with a <see cref="DateLogicalType" /> or a
        /// <see cref="StringSchema" /> based on the value of <see cref="TemporalBehavior" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="IntSchema" /> and
        /// <see cref="DateLogicalType" /> or <see cref="StringSchema" /> if <paramref name="type" />
        /// is <see cref="DateOnly" />; an unsuccessful <see cref="SchemaBuilderCaseResult" /> with
        /// an <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(DateOnly))
            {
                Schema dateSchema = TemporalBehavior switch
                {
                    TemporalBehavior.EpochMicroseconds or TemporalBehavior.EpochMilliseconds => new IntSchema()
                    {
                        LogicalType = new DateLogicalType(),
                    },
                    TemporalBehavior.Iso8601 => new StringSchema(),
                    _ => throw new ArgumentOutOfRangeException(nameof(TemporalBehavior)),
                };

                try
                {
                    context.Schemas.Add(type, dateSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(dateSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(DateSchemaBuilderCase)} can only be applied to the {nameof(DateOnly)} type."));
            }
        }
    }
}
#endif
