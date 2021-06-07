namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="UnionSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonUnionDeserializerBuilderCase : UnionDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonUnionDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build child deserializers.
        /// </param>
        public JsonUnionDeserializerBuilderCase(IJsonDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "JSON deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build child deserializers.
        /// </summary>
        public IJsonDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="UnionSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="UnionSchema" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
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
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is UnionSchema unionSchema)
            {
                if (unionSchema.Schemas.Count < 1)
                {
                    throw new UnsupportedSchemaException(schema, "A deserializer cannot be built for an empty union.");
                }

                var tokenType = typeof(Utf8JsonReader)
                    .GetProperty(nameof(Utf8JsonReader.TokenType));

                var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                var read = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.Read), Type.EmptyTypes);

                var getString = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                var getUnknownUnionMemberException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnknownUnionMemberException));

                var schemas = unionSchema.Schemas.ToList();
                var candidates = schemas.Where(s => !(s is NullSchema)).ToList();
                var @null = schemas.Find(s => s is NullSchema);

                var cases = candidates.Select(child =>
                {
                    var selected = SelectType(type, child);

                    return Expression.SwitchCase(
                        BuildConversion(
                            Expression.Block(
                                Expression.Call(context.Reader, read),
                                DeserializerBuilder.BuildExpression(selected, child, context)),
                            type),
                        Expression.Constant(GetSchemaName(child)));
                }).ToArray();

                var value = Expression.Parameter(type);

                Expression expression = Expression.Block(
                    new[] { value },
                    Expression.IfThen(
                        Expression.NotEqual(
                            Expression.Property(context.Reader, tokenType),
                            Expression.Constant(JsonTokenType.StartObject)),
                        Expression.Throw(
                            Expression.Call(
                                null,
                                getUnexpectedTokenException,
                                context.Reader,
                                Expression.Constant(new[] { JsonTokenType.StartObject })))),
                    Expression.Call(context.Reader, read),
                    Expression.Assign(
                        value,
                        Expression.Switch(
                            Expression.Call(context.Reader, getString),
                            Expression.Throw(
                                Expression.Call(
                                    null,
                                    getUnknownUnionMemberException,
                                    context.Reader),
                                type),
                            cases)),
                    Expression.Call(context.Reader, read),
                    Expression.IfThen(
                        Expression.NotEqual(
                            Expression.Property(context.Reader, tokenType),
                            Expression.Constant(JsonTokenType.EndObject)),
                        Expression.Throw(
                            Expression.Call(
                                null,
                                getUnexpectedTokenException,
                                context.Reader,
                                Expression.Constant(new[] { JsonTokenType.EndObject })))),
                    value);

                if (@null != null)
                {
                    var selected = SelectType(type, @null);
                    var underlying = Nullable.GetUnderlyingType(selected);

                    if (selected.IsValueType && underlying == null)
                    {
                        throw new UnsupportedTypeException(type, $"A deserializer for a union containing {typeof(NullSchema)} cannot be built for {selected}.");
                    }

                    expression = Expression.Condition(
                        Expression.Equal(
                            Expression.Property(context.Reader, tokenType),
                            Expression.Constant(JsonTokenType.Null)),
                        BuildConversion(
                            DeserializerBuilder.BuildExpression(selected, @null, context),
                            type),
                        expression);
                }

                return JsonDeserializerBuilderCaseResult.FromExpression(expression);
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonUnionDeserializerBuilderCase)} can only be applied to {nameof(UnionSchema)}s."));
            }
        }

        /// <summary>
        /// Gets the name of the property used to disambiguate a union.
        /// </summary>
        /// <param name="schema">
        /// A child of the union schema.
        /// </param>
        /// <returns>
        /// If <paramref name="schema" /> is a <see cref="NamedSchema" />, the fully-qualified
        /// name; the type name otherwise.
        /// </returns>
        protected virtual string GetSchemaName(Schema schema)
        {
            return schema switch
            {
                NamedSchema namedSchema => namedSchema.FullName,

                ArraySchema => JsonSchemaToken.Array,
                BooleanSchema => JsonSchemaToken.Boolean,
                BytesSchema => JsonSchemaToken.Bytes,
                DoubleSchema => JsonSchemaToken.Double,
                FloatSchema => JsonSchemaToken.Float,
                IntSchema => JsonSchemaToken.Int,
                LongSchema => JsonSchemaToken.Long,
                MapSchema => JsonSchemaToken.Map,
                StringSchema => JsonSchemaToken.String,

                _ => throw new UnsupportedSchemaException(schema)
            };
        }
    }
}
