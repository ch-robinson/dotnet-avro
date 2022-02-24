namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="ArraySchema" />
    /// and attempts to map it to enumerable types.
    /// </summary>
    public class JsonArraySerializerBuilderCase : ArraySerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonArraySerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build item serializers.
        /// </param>
        public JsonArraySerializerBuilderCase(IJsonSerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "JSON serializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build item serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for an <see cref="ArraySchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="type" />
        /// is an enumerable type and <paramref name="schema" /> is an <see cref="ArraySchema" />;
        /// an unsuccessful <see cref="JsonSerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> does not implement <see cref="IEnumerable{T}" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is ArraySchema arraySchema)
            {
                var enumerableType = GetEnumerableType(type);

                if (enumerableType is not null || type == typeof(object))
                {
                    // support dynamic mapping:
                    var itemType = enumerableType ?? typeof(object);

                    var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(itemType));
                    var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(itemType));
                    var loop = Expression.Label();

                    var writeStartArray = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStartArray), Type.EmptyTypes);

                    var getEnumerator = enumerable.Type
                        .GetMethod("GetEnumerator", Type.EmptyTypes);

                    var getCurrent = enumerator.Type
                        .GetProperty(nameof(IEnumerator.Current))
                        .GetGetMethod();

                    var moveNext = typeof(IEnumerator)
                        .GetMethod(nameof(IEnumerator.MoveNext), Type.EmptyTypes);

                    var writeItem = SerializerBuilder
                        .BuildExpression(Expression.Property(enumerator, getCurrent), arraySchema.Item, context);

                    var writeEndArray = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteEndArray), Type.EmptyTypes);

                    var dispose = typeof(IDisposable)
                        .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

                    Expression expression;

                    try
                    {
                        expression = BuildConversion(value, enumerable.Type);
                    }
                    catch (Exception exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {arraySchema} to {type}.", exception);
                    }

                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Block(
                            new[] { enumerator },
                            Expression.Call(context.Writer, writeStartArray),
                            Expression.Assign(
                                enumerator,
                                Expression.Call(expression, getEnumerator)),
                            Expression.TryFinally(
                                Expression.Loop(
                                    Expression.IfThenElse(
                                        Expression.Call(enumerator, moveNext),
                                        writeItem,
                                        Expression.Break(loop)),
                                    loop),
                                Expression.Call(enumerator, dispose)),
                            Expression.Call(context.Writer, writeEndArray)));
                }
                else
                {
                    return JsonSerializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(JsonArraySerializerBuilderCase)} can only be applied to enumerable types."));
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonArraySerializerBuilderCase)} can only be applied to {nameof(ArraySchema)}s."));
            }
        }
    }
}
