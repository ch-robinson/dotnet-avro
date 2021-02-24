namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

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
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is an <see ref="EnumResolution" /> and <paramref name="schema" /> is an <see cref="EnumSchema" />;
        /// an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="resolution" /> does not contain a matching symbol for each
        /// symbol in <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var readInteger = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                    Expression expression = Expression.ConvertChecked(
                        Expression.Call(context.Reader, readInteger),
                        typeof(int));

                    // find a match for each enum in the schema:
                    var cases = enumSchema.Symbols.Select((name, index) =>
                    {
                        var match = enumResolution.Symbols.SingleOrDefault(s => s.Name.IsMatch(name));

                        if (match == null)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has no value that matches {name}.");
                        }

                        return Expression.SwitchCase(
                            BuildConversion(Expression.Constant(match.Value), resolution.Type),
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
                                    resolution.Type),
                                cases.ToArray()));
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new UnsupportedTypeException(resolution.Type, $"Failed to map {enumSchema} to {resolution.Type}.", exception);
                    }
                }
                else
                {
                    return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(BinaryEnumDeserializerBuilderCase)} can only be applied to {nameof(EnumResolution)}s."));
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryEnumDeserializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
