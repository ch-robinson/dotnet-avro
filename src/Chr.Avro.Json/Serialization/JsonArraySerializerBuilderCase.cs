namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

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
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is an <see ref="ArrayResolution" /> and <paramref name="schema" /> is a <see cref="ArraySchema" />;
        /// an unsuccessful <see cref="JsonSerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved <see cref="Type" /> does not implement <see cref="IEnumerable{T}" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is ArraySchema arraySchema)
            {
                if (resolution is ArrayResolution arrayResolution)
                {
                    var itemType = arrayResolution.ItemType;
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

                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Block(
                            new[] { enumerator },
                            Expression.Call(context.Writer, writeStartArray),
                            Expression.Assign(enumerator, Expression.Call(value, getEnumerator)),
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
                    return JsonSerializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(JsonArraySerializerBuilderCase)} can only be applied to {nameof(ArrayResolution)}s."));
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonArraySerializerBuilderCase)} can only be applied to {nameof(ArraySchema)}s."));
            }
        }
    }
}
