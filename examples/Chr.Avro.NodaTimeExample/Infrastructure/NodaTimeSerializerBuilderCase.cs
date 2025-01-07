namespace Chr.Avro.NodaTimeExample.Infrastructure
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;
    using NodaTime;
    using NodaTime.Text;

    public class NodaTimeSerializerBuilderCase : BinaryTimestampSerializerBuilderCase
    {
        public override BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (type != typeof(Instant))
            {
                return BinarySerializerBuilderCaseResult.FromException(
                    new UnsupportedTypeException(type, $"{nameof(NodaTimeSerializerBuilderCase)} can only be applied to {nameof(Instant)}."));
            }

            if (schema is StringSchema stringSchema)
            {
                var format = typeof(IPattern<Instant>)
                    .GetMethod(nameof(IPattern<Instant>.Format), new[] { typeof(Instant) })!;

                var writeString = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteString), new[] { typeof(string) })!;

                try
                {
                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Call(
                            context.Writer,
                            writeString,
                            Expression.Call(
                                Expression.Constant(InstantPattern.ExtendedIso),
                                format,
                                BuildConversion(value, typeof(Instant)))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {stringSchema} to {type}.", exception);
                }
            }
            else if (schema.LogicalType is TimestampLogicalType)
            {
                if (schema is not LongSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(TimestampLogicalType)} deserializers can only be built for {nameof(LongSchema)}s.");
                }

                Expression expression;

                try
                {
                    expression = BuildConversion(value, typeof(Instant));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }

                var secondsAndNanoseconds = Expression.Variable(typeof(Tuple<long, int>));
                var seconds = secondsAndNanoseconds.Type.GetProperty(nameof(Tuple<long, int>.Item1));
                var nanoseconds = secondsAndNanoseconds.Type.GetProperty(nameof(Tuple<long, int>.Item2));

                var toUnixTimeSecondsAndNanoseconds = typeof(Instant)
                    .GetMethod(nameof(Instant.ToUnixTimeSecondsAndNanoseconds), Type.EmptyTypes)!;

                var toUnixTimeTicks = typeof(Instant)
                    .GetMethod(nameof(Instant.ToUnixTimeTicks), Type.EmptyTypes)!;

                expression = schema.LogicalType switch
                {
                    NanosecondTimestampLogicalType =>
                        Expression.Block(
                            new[] { secondsAndNanoseconds },
                            Expression.Assign(
                                secondsAndNanoseconds,
                                Expression.Call(expression, toUnixTimeSecondsAndNanoseconds)),
                            Expression.AddChecked(
                                Expression.MultiplyChecked(
                                    Expression.Property(secondsAndNanoseconds, seconds),
                                    Expression.Constant(1000000000L)),
                                Expression.Convert(
                                    Expression.Property(secondsAndNanoseconds, nanoseconds),
                                    typeof(long)))),
                    _ =>
                        BuildTicksToTimestamp(
                            Expression.Call(expression, toUnixTimeTicks),
                            schema),
                };

                return BinarySerializerBuilderCaseResult.FromExpression(expression);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(
                    new UnsupportedSchemaException(schema, $"{nameof(NodaTimeSerializerBuilderCase)} can only be applied to {nameof(StringSchema)}s or schemas with a {nameof(TimestampLogicalType)}."));
            }
        }
    }
}
