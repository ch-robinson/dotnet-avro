namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="UnionSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryUnionDeserializerBuilderCase : UnionDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryUnionDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build child deserializers.
        /// </param>
        public BinaryUnionDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build child deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="UnionSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="UnionSchema" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> has no <see cref="UnionSchema.Schemas" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be mapped to each <see cref="Schema" /> in
        /// <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is UnionSchema unionSchema)
            {
                if (unionSchema.Schemas.Count < 1)
                {
                    throw new UnsupportedSchemaException(schema, "A deserializer cannot be built for an empty union.");
                }

                var readInteger = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                Expression expression = Expression.Call(context.Reader, readInteger);

                // create a mapping for each schema in the union:
                var cases = unionSchema.Schemas.Select((child, index) =>
                {
                    var selected = SelectType(type, child);
                    var underlying = Nullable.GetUnderlyingType(selected);

                    if (child is NullSchema && selected.IsValueType && underlying == null)
                    {
                        throw new UnsupportedTypeException(type, $"A deserializer for {unionSchema} cannot be built for {selected} because it contains {nameof(NullSchema)}.");
                    }

                    var @case = DeserializerBuilder.BuildExpression(selected, child, context);

                    return Expression.SwitchCase(
                        BuildConversion(@case, type),
                        Expression.Constant((long)index));
                });

                var position = typeof(BinaryReader)
                    .GetProperty(nameof(BinaryReader.Index));

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
                                    Expression.Constant($"Invalid union index; expected a value in [0-{unionSchema.Schemas.Count}). This may indicate invalid encoding earlier in the stream."),
                                    Expression.Constant(null, typeof(Exception))),
                                type),
                            cases.ToArray()));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {unionSchema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryUnionDeserializerBuilderCase)} can only be applied to {nameof(UnionSchema)}s."));
            }
        }
    }
}
