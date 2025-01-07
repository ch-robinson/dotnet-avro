namespace Chr.Avro.NodaTimeExample.Infrastructure
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using NodaTime;
    using NodaTime.Text;

    public class NodaTimeDeserializerBuilderCase : BinaryTimestampDeserializerBuilderCase
    {
        public override BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (type != typeof(Instant))
            {
                return BinaryDeserializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(NodaTimeDeserializerBuilderCase)} can only be applied to {nameof(Instant)}."));
            }

            if (schema is StringSchema)
            {
                var readString = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadString), Type.EmptyTypes)!;

                var parse = typeof(IPattern<Instant>)
                    .GetMethod(nameof(IPattern<Instant>.Parse), new[] { typeof(string) })!;

                var getValueOrThrow = typeof(ParseResult<Instant>)
                    .GetMethod(nameof(ParseResult<Instant>.GetValueOrThrow), Type.EmptyTypes)!;

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Call(
                                Expression.Call(
                                    Expression.Constant(InstantPattern.ExtendedIso),
                                    parse,
                                    Expression.Call(context.Reader, readString)),
                                getValueOrThrow),
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

                var readInteger = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes)!;

                var plusNanoseconds = typeof(Instant)
                    .GetMethod(nameof(Instant.PlusNanoseconds), new[] { typeof(long) })!;

                var plusTicks = typeof(Instant)
                    .GetMethod(nameof(Instant.PlusTicks), new[] { typeof(long) })!;

                Expression epoch = Expression.Constant(Instant.FromDateTimeOffset(Epoch));
                Expression expression = Expression.Call(context.Reader, readInteger);

                // avoid losing nanosecond precision
                expression = schema.LogicalType switch
                {
                    NanosecondTimestampLogicalType =>
                        Expression.Call(epoch, plusNanoseconds, expression),
                    _ =>
                        Expression.Call(epoch, plusTicks, BuildTimestampToTicks(expression, schema)),
                };

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(expression, type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(
                    new UnsupportedSchemaException(schema, $"{nameof(NodaTimeDeserializerBuilderCase)} can only be applied to {nameof(StringSchema)}s or schemas with a {nameof(TimestampLogicalType)}."));
            }
        }
    }
}
