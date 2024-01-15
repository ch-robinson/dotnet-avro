namespace Chr.Avro.NodaTimeExample.Infrastructure
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using NodaTime;

    public class NodaTimeDeserializerBuilderCase : BinaryTimestampDeserializerBuilderCase
    {
        private readonly BinaryStringDeserializerBuilderCase stringDeserializer = new();

        public override BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is StringSchema)
            {
                // Fallback to default implementation if not NodaTime
                if (!(type == typeof(NodaTime.Instant) || type == typeof(NodaTime.Instant?)))
                {
                    return base.BuildExpression(type, schema, context);
                }

                // Use default conversion from string to DateTimeOffset
                var dateTimeOffset = stringDeserializer.BuildExpression(typeof(DateTimeOffset), schema, context).Expression;

                var instantFromDateTimeOffset = typeof(Instant).GetMethod(nameof(Instant.FromDateTimeOffset), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public, new[] { typeof(DateTimeOffset) });

                try
                {
                    // Code: NodaTime.Instant.FromDateTimeOffset(dateTimeOffset);
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Call(instantFromDateTimeOffset!, dateTimeOffset!),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else if (schema.LogicalType is TimestampLogicalType)
            {
                if (schema is not LongSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(TimestampLogicalType)} deserializers can only be built for {nameof(LongSchema)}s.");
                }

                // Fallback to default implementation if not NodaTime
                if (!(type == typeof(Instant) || type == typeof(Instant?)))
                {
                    return base.BuildExpression(type, schema, context);
                }

                var factor = schema.LogicalType switch
                {
                    MicrosecondTimestampLogicalType => TimeSpan.TicksPerMillisecond / 1000,
                    MillisecondTimestampLogicalType => TimeSpan.TicksPerMillisecond,
                    _ => throw new UnsupportedSchemaException(schema, $"{schema.LogicalType} is not a supported {nameof(TimestampLogicalType)}."),
                };

                var readInteger = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                Expression expression = Expression.Call(context.Reader, readInteger!);

                var addTicks = typeof(DateTime)
                    .GetMethod(nameof(DateTime.AddTicks), new[] { typeof(long) });

                var fromDateTimeUtc = typeof(NodaTime.Instant).GetMethod(nameof(NodaTime.Instant.FromDateTimeUtc), new[] { typeof(DateTime) });

                try
                {
                    // Code: return NodaTime.Instant.FromDateTimeUtc( Epoch.AddTicks(value * factor) );
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Call(
                                fromDateTimeUtc!,
                                Expression.Call(
                                    Expression.Constant(Epoch),
                                    addTicks!,
                                    Expression.Multiply(expression, Expression.Constant(factor)))),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(NodaTimeDeserializerBuilderCase)} can only be applied to schemas with a {nameof(TimestampLogicalType)} and NodaTime properties."));
            }
        }
    }
}
