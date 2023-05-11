namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="MapSchema" />
    /// and attempts to map it to dictionary types.
    /// </summary>
    public class JsonMapSerializerBuilderCase : MapSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonMapSerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build key and value serializers.
        /// </param>
        public JsonMapSerializerBuilderCase(IJsonSerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "JSON serializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build key and value serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for an <see cref="MapSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="type" />
        /// is a dictionary type and <paramref name="schema" /> is a <see cref="MapSchema" />; an
        /// unsuccessful <see cref="JsonSerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> does not implement <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context, bool registerExpression)
        {
            if (schema is MapSchema mapSchema)
            {
                var dictionaryTypes = GetDictionaryTypes(type);

                if (dictionaryTypes is not null || type == typeof(object))
                {
                    // support dynamic mapping:
                    var keyType = dictionaryTypes?.Key ?? typeof(object);
                    var valueType = dictionaryTypes?.Value ?? typeof(object);

                    var pairType = typeof(KeyValuePair<,>).MakeGenericType(keyType, valueType);
                    var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(pairType));
                    var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(pairType));
                    var loop = Expression.Label();

                    var writeStartObject = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStartObject), Type.EmptyTypes);

                    var getEnumerator = enumerable.Type
                        .GetMethod("GetEnumerator", Type.EmptyTypes);

                    var getCurrent = enumerator.Type
                        .GetProperty(nameof(IEnumerator.Current))
                        .GetGetMethod();

                    var moveNext = typeof(IEnumerator)
                        .GetMethod(nameof(IEnumerator.MoveNext), Type.EmptyTypes);

                    var getKey = pairType
                        .GetProperty("Key")
                        .GetGetMethod();

                    var getValue = pairType
                        .GetProperty("Value")
                        .GetGetMethod();

                    var writeKey = new KeySerializerVisitor().Visit(
                        SerializerBuilder.BuildExpression(
                            Expression.Property(Expression.Property(enumerator, getCurrent), getKey), new StringSchema(), context, registerExpression));

                    var writeValue = SerializerBuilder.BuildExpression(
                        Expression.Property(Expression.Property(enumerator, getCurrent), getValue), mapSchema.Value, context, registerExpression);

                    var writeEndObject = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteEndObject), Type.EmptyTypes);

                    var dispose = typeof(IDisposable)
                        .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

                    Expression expression;

                    try
                    {
                        expression = BuildConversion(value, enumerable.Type);
                    }
                    catch (Exception exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {mapSchema} to {type}.", exception);
                    }

                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Block(
                            new[] { enumerator },
                            Expression.Call(context.Writer, writeStartObject),
                            Expression.Assign(
                                enumerator,
                                Expression.Call(expression, getEnumerator)),
                            Expression.TryFinally(
                                Expression.Loop(
                                    Expression.IfThenElse(
                                        Expression.Call(enumerator, moveNext),
                                        Expression.Block(writeKey, writeValue),
                                        Expression.Break(loop)),
                                    loop),
                                Expression.Call(enumerator, dispose)),
                            Expression.Call(context.Writer, writeEndObject)));
                }
                else
                {
                    return JsonSerializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(JsonMapSerializerBuilderCase)} can only be applied to dictionary types."));
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonMapSerializerBuilderCase)} can only be applied to {nameof(MapSchema)}s."));
            }
        }

        /// <summary>
        /// Visits a key serializer to rewrite <see cref="Utf8JsonWriter.WriteStringValue(string)" />
        /// calls.
        /// </summary>
        protected class KeySerializerVisitor : ExpressionVisitor
        {
            private static readonly MethodInfo WritePropertyName = typeof(Utf8JsonWriter)
                .GetMethod(nameof(Utf8JsonWriter.WritePropertyName), new[] { typeof(string) });

            private static readonly MethodInfo WriteString = typeof(Utf8JsonWriter)
                .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(string) });

            /// <summary>
            /// Rewrites a <see cref="Utf8JsonWriter.WriteStringValue(string)" /> call to
            /// <see cref="Utf8JsonWriter.WritePropertyName(string)" />.
            /// </summary>
            /// <inheritdoc />
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method == WriteString)
                {
                    return Expression.Call(node.Object, WritePropertyName, node.Arguments);
                }

                return node;
            }
        }
    }
}
