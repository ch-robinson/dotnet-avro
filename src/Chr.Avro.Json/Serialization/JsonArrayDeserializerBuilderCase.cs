namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="ArraySchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonArrayDeserializerBuilderCase : ArrayDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonArrayDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build item deserializers.
        /// </param>
        public JsonArrayDeserializerBuilderCase(IJsonDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "JSON deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build item deserializers.
        /// </summary>
        public IJsonDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for an <see cref="ArraySchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is an enumerable type and <paramref name="schema" /> is an <see cref="ArraySchema" />;
        /// an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> is not assignable from any supported array or
        /// collection <see cref="Type" /> and does not have a constructor that can be used to
        /// instantiate it.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is ArraySchema arraySchema)
            {
                var enumerableType = GetEnumerableType(type);

                if (enumerableType is not null || type == typeof(object))
                {
                    // support dynamic mapping:
                    var itemType = enumerableType ?? typeof(object);

                    var instantiateCollection = BuildIntermediateCollection(type, itemType);

                    var readItem = DeserializerBuilder
                        .BuildExpression(itemType, arraySchema.Item, context);

                    var collection = Expression.Parameter(instantiateCollection.Type);
                    var loop = Expression.Label();

                    var tokenType = typeof(Utf8JsonReader)
                        .GetProperty(nameof(Utf8JsonReader.TokenType));

                    var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                        .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                    var read = typeof(Utf8JsonReader)
                        .GetMethod(nameof(Utf8JsonReader.Read), Type.EmptyTypes);

                    var add = collection.Type.GetMethod("Add", new[] { readItem.Type });

                    Expression expression = Expression.Block(
                        new[] { collection },
                        Expression.IfThen(
                            Expression.NotEqual(
                                Expression.Property(context.Reader, tokenType),
                                Expression.Constant(JsonTokenType.StartArray)),
                            Expression.Throw(
                                Expression.Call(
                                    null,
                                    getUnexpectedTokenException,
                                    context.Reader,
                                    Expression.Constant(new[] { JsonTokenType.StartArray })))),
                        Expression.Assign(collection, instantiateCollection),
                        Expression.Loop(
                            Expression.Block(
                                Expression.Call(context.Reader, read),
                                Expression.IfThen(
                                    Expression.Equal(
                                        Expression.Property(context.Reader, tokenType),
                                        Expression.Constant(JsonTokenType.EndArray)),
                                    Expression.Break(loop)),
                                Expression.Call(collection, add, readItem)),
                            loop),
                        collection);

                    if (!type.IsAssignableFrom(expression.Type) && GetCollectionConstructor(type) is ConstructorInfo constructor)
                    {
                        expression = Expression.New(constructor, expression);
                    }

                    try
                    {
                        return JsonDeserializerBuilderCaseResult.FromExpression(
                            BuildConversion(expression, type));
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {arraySchema} to {type}.", exception);
                    }
                }
                else
                {
                    return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(JsonArrayDeserializerBuilderCase)} can only be applied to enumerable types."));
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonArrayDeserializerBuilderCase)} can only be applied to {nameof(ArraySchema)}s."));
            }
        }
    }
}
