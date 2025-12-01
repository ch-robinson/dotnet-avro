namespace Chr.Avro.Serialization
{
    using System;
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
                var itemType = GetEnumerableType(type);

                if (itemType is not null || type == typeof(object))
                {
                    // support dynamic mapping:
                    itemType ??= typeof(object);
                    var readOnlyCollectionType = typeof(IReadOnlyCollection<>).MakeGenericType(itemType);
                    var enumerableType = typeof(IEnumerable<>).MakeGenericType(itemType);
                    var enumeratorType = typeof(IEnumerator<>).MakeGenericType(itemType);

                    Expression expression;
                    try
                    {
                        if (readOnlyCollectionType.IsAssignableFrom(type))
                        {
                            // NOTE: Not casting the expression to allow us to get the specific enumerator of `type`
                            // This way we can avoid the allocation of an IEnumerator<T> and the overhead of
                            // virtual dispatch calls
                            expression = value;
                        }
                        else
                        {
                            expression = BuildConversion(value, readOnlyCollectionType);
                        }
                    }
                    catch (Exception exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {arraySchema} to {type}.", exception);
                    }

                    var collection = Expression.Variable(expression.Type);
                    var enumerationReflection = EnumerationReflection.Create(collection, readOnlyCollectionType, enumerableType);

                    var loopLabel = Expression.Label();

                    var writeStartArray = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStartArray), Type.EmptyTypes);

                    var writeItem = SerializerBuilder
                        .BuildExpression(Expression.Property(enumerationReflection.Enumerator, enumerationReflection.GetCurrent), arraySchema.Item, context);

                    var writeEndArray = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteEndArray), Type.EmptyTypes);

                    Expression loop = Expression.Loop(
                        Expression.IfThenElse(
                            Expression.Call(enumerationReflection.Enumerator, enumerationReflection.MoveNext),
                            writeItem,
                            Expression.Break(loopLabel)),
                        loopLabel);

                    if (enumerationReflection.DisposeCall is not null)
                    {
                        loop = Expression.TryFinally(loop, enumerationReflection.DisposeCall);
                    }

                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Block(
                            new[] { enumerationReflection.Enumerator },
                            Expression.Call(context.Writer, writeStartArray),
                            Expression.Assign(
                                enumerationReflection.Enumerator,
                                Expression.Call(expression, enumerationReflection.GetEnumerator)),
                            loop,
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
