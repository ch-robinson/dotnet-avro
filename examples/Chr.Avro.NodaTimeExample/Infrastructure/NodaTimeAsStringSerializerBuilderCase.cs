namespace Chr.Avro.NodaTimeExample.Infrastructure
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    public class NodaTimeAsStringSerializerBuilderCase : BinaryStringSerializerBuilderCase
    {
        public override BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is StringSchema stringSchema)
            {
                if (value.Type != typeof(NodaTime.Instant))
                {
                    return BinarySerializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(NodaTimeAsStringSerializerBuilderCase)} can only be applied to {nameof(StringSchema)}s."));
                }

                var writeString = typeof(BinaryWriter).GetMethod(nameof(BinaryWriter.WriteString), new[] { typeof(string) });

                try
                {
                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeString!, GetExpression(value, typeof(string))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {stringSchema} to {type}.", exception);
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryStringSerializerBuilderCase)} can only be applied to {nameof(StringSchema)}s."));
            }
        }

        private Expression GetExpression(Expression value, Type target)
        {
            var methodInfo = typeof(NodaTime.Instant).GetMethod(nameof(NodaTime.Instant.ToDateTimeOffset), Array.Empty<Type>())
                ?? throw new NullReferenceException("Tried to find the method 'NodaTime.Instant.ToDateTimeOffset()', but it seem to have disappeared.");

            // This is: value = value.ToDateTimeOffset();
            // The base class implementation will fruther handle the value as normal DateTimeOffset.
            value = Expression.Call(value, methodInfo);
            return BuildStaticConversion(value, target);
        }
    }
}
