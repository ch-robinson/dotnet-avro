namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="DurationLogicalType" />
    /// and attempts to map it to <see cref="TimeSpan" />.
    /// </summary>
    public class JsonDurationDeserializerBuilderCase : DurationDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="DurationLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful result if <paramref name="type" /> the schema’s logical type is a
        /// <see cref="DurationLogicalType" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="FixedSchema" /> with size 12 and logical
        /// type <see cref="DurationLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="TimeSpan" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema.LogicalType is DurationLogicalType)
            {
                if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var bytes = Expression.Parameter(typeof(byte[]));

                var copy = typeof(Array)
                    .GetMethod(nameof(Array.Copy), new[] { typeof(Array), typeof(int), typeof(Array), typeof(int), typeof(int) });

                var tokenType = typeof(Utf8JsonReader)
                    .GetProperty(nameof(Utf8JsonReader.TokenType));

                var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                var getString = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                var getBytes = typeof(Encoding)
                    .GetMethod(nameof(Encoding.GetBytes), new[] { typeof(string) });

                var getUnexpectedSizeException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedSizeException));

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                var toUInt32 = typeof(BitConverter)
                    .GetMethod(nameof(BitConverter.ToUInt32), new[] { typeof(byte[]), typeof(int) });

                Expression Read(Expression offset)
                {
                    var component = Expression.Variable(typeof(byte[]));

                    var expressions = new List<Expression>
                    {
                        Expression.Assign(
                            component,
                            Expression.NewArrayBounds(typeof(byte), Expression.Constant(4))),
                        Expression.Call(null, copy, bytes, offset, component, Expression.Constant(0), Expression.ArrayLength(component)),
                    };

                    if (!BitConverter.IsLittleEndian)
                    {
                        expressions.Add(Expression.Call(null, reverse, Expression.Convert(component, typeof(Array))));
                    }

                    expressions.Add(component);

                    return Expression.ConvertChecked(
                        Expression.Call(
                            null,
                            toUInt32,
                            Expression.Block(
                                new[] { component },
                                expressions),
                            Expression.Constant(0)),
                        typeof(long));
                }

                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                var timeSpanConstructor = typeof(TimeSpan)
                    .GetConstructor(new[] { typeof(long) });

                try
                {
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Block(
                            new[] { bytes },
                            Expression.IfThen(
                                Expression.NotEqual(
                                    Expression.Property(context.Reader, tokenType),
                                    Expression.Constant(JsonTokenType.String)),
                                Expression.Throw(
                                    Expression.Call(
                                        null,
                                        getUnexpectedTokenException,
                                        context.Reader,
                                        Expression.Constant(new[] { JsonTokenType.String })))),
                            Expression.Assign(
                                bytes,
                                Expression.Call(
                                    Expression.Constant(JsonEncoding.Bytes),
                                    getBytes,
                                    Expression.Call(context.Reader, getString))),
                            Expression.IfThen(
                                Expression.NotEqual(
                                    Expression.ArrayLength(bytes),
                                    Expression.Constant(fixedSchema.Size)),
                                Expression.Throw(
                                    Expression.Call(
                                        null,
                                        getUnexpectedSizeException,
                                        context.Reader,
                                        Expression.Constant(fixedSchema.Size),
                                        Expression.ArrayLength(bytes)))),
                            Expression.IfThen(
                                Expression.NotEqual(Read(Expression.Constant(0)), Expression.Constant(0L)),
                                Expression.Throw(
                                    Expression.New(
                                        exceptionConstructor,
                                        Expression.Constant("Durations containing months cannot be accurately deserialized to a TimeSpan.")))),
                            Expression.New(
                                timeSpanConstructor,
                                Expression.AddChecked(
                                    Expression.MultiplyChecked(Read(Expression.Constant(4)), Expression.Constant(TimeSpan.TicksPerDay)),
                                    Expression.MultiplyChecked(Read(Expression.Constant(8)), Expression.Constant(TimeSpan.TicksPerMillisecond))))),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDurationDeserializerBuilderCase)} can only be applied schemas with a {nameof(DurationLogicalType)}."));
            }
        }
    }
}
