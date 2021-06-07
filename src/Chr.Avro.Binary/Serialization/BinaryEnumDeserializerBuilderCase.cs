namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="EnumSchema" />
    /// and attempts to map it to enum types.
    /// </summary>
    public class BinaryEnumDeserializerBuilderCase : EnumDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is an enum and <paramref name="schema" /> is an <see cref="EnumSchema" />; an
        /// unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> does not have a matching member for each symbol in
        /// <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                var underlying = Nullable.GetUnderlyingType(type) ?? type;

                if (underlying.IsEnum)
                {
                    var readInteger = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                    Expression expression = Expression.ConvertChecked(
                        Expression.Call(context.Reader, readInteger),
                        typeof(int));

                    // enum fields will always be public static, so no need to expose binding flags:
                    var fields = underlying.GetFields(BindingFlags.Public | BindingFlags.Static);

                    // find a match for each enum in the schema:
                    var cases = enumSchema.Symbols.Select((symbol, index) =>
                    {
                        var match = fields.SingleOrDefault(field => IsMatch(symbol, field.Name));

                        if (match == null)
                        {
                            throw new UnsupportedTypeException(type, $"{type} has no value that matches {symbol}.");
                        }

                        return Expression.SwitchCase(
                            BuildConversion(Expression.Constant(Enum.Parse(underlying, match.Name)), type),
                            Expression.Constant(index));
                    });

                    var position = typeof(BinaryReader)
                        .GetProperty(nameof(BinaryReader.Index))
                        .GetGetMethod();

                    var exceptionConstructor = typeof(InvalidEncodingException)
                        .GetConstructor(new[] { typeof(long), typeof(string), typeof(Exception) });

                    try
                    {
                        // generate a switch on the index:
                        return BinaryDeserializerBuilderCaseResult.FromExpression(
                            Expression.Switch(
                                expression,
                                Expression.Throw(
                                    Expression.New(
                                        exceptionConstructor,
                                        Expression.Property(context.Reader, position),
                                        Expression.Constant($"Invalid enum index; expected a value in [0-{enumSchema.Symbols.Count}). This may indicate invalid encoding earlier in the stream."),
                                        Expression.Constant(null, typeof(Exception))),
                                    type),
                                cases.ToArray()));
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {enumSchema} to {type}.", exception);
                    }
                }
                else
                {
                    return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryEnumDeserializerBuilderCase)} can only be applied to enum types."));
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryEnumDeserializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
