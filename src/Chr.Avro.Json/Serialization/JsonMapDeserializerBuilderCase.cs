namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="MapSchema" />
    /// and attempts to map it to dictionary types.
    /// </summary>
    public class JsonMapDeserializerBuilderCase : MapDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonMapDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build key and value deserializers.
        /// </param>
        public JsonMapDeserializerBuilderCase(IJsonDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "JSON deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build key and value deserializers.
        /// </summary>
        public IJsonDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="MapSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is a <see ref="MapResolution" /> and <paramref name="schema" /> is a <see cref="MapSchema" />;
        /// an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved <see cref="Type" /> is not assignable from any supported
        /// dictionary <see cref="Type" /> and does not have a constructor that can be used to
        /// instantiate it.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is MapSchema mapSchema)
            {
                if (resolution is MapResolution mapResolution)
                {
                    var instantiateDictionary = BuildIntermediateDictionary(mapResolution);

                    var readKey = DeserializerBuilder
                        .BuildExpression(mapResolution.KeyType, new StringSchema(), context);

                    var readValue = DeserializerBuilder
                        .BuildExpression(mapResolution.ValueType, mapSchema.Value, context);

                    var dictionary = Expression.Parameter(instantiateDictionary.Type);
                    var key = Expression.Parameter(readKey.Type);
                    var loop = Expression.Label();

                    var tokenType = typeof(Utf8JsonReader)
                        .GetProperty(nameof(Utf8JsonReader.TokenType));

                    var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                        .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                    var read = typeof(Utf8JsonReader)
                        .GetMethod(nameof(Utf8JsonReader.Read), Type.EmptyTypes);

                    var add = dictionary.Type.GetMethod("Add", new[] { readKey.Type, readValue.Type });

                    Expression expression = Expression.Block(
                        new[] { dictionary },
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
                        Expression.Assign(dictionary, instantiateDictionary),
                        Expression.Loop(
                            Expression.Block(
                                new[] { key },
                                Expression.Call(context.Reader, read),
                                Expression.IfThen(
                                    Expression.Equal(
                                        Expression.Property(context.Reader, tokenType),
                                        Expression.Constant(JsonTokenType.EndObject)),
                                    Expression.Break(loop)),
                                Expression.Assign(key, readKey),
                                Expression.Call(context.Reader, read),
                                Expression.Call(dictionary, add, key, readValue)),
                            loop),
                        dictionary);

                    if (!mapResolution.Type.IsAssignableFrom(expression.Type) && FindDictionaryConstructor(mapResolution) is ConstructorResolution constructorResolution)
                    {
                        expression = Expression.New(constructorResolution.Constructor, new[] { expression });
                    }

                    try
                    {
                        return JsonDeserializerBuilderCaseResult.FromExpression(
                            BuildConversion(expression, mapResolution.Type));
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new UnsupportedTypeException(resolution.Type, $"Failed to map {mapSchema} to {resolution.Type}.", exception);
                    }
                }
                else
                {
                    return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(JsonMapDeserializerBuilderCase)} can only be applied to {nameof(MapResolution)}s."));
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonMapDeserializerBuilderCase)} can only be applied to {nameof(MapSchema)}s."));
            }
        }
    }
}
