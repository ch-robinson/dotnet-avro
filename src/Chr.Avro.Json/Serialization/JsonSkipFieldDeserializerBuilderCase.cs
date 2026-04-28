namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that skips over JSON-encoded
    /// fields without deserializing them.
    /// </summary>
    internal class JsonSkipFieldDeserializerBuilderCase : DeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> that skips the field.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is <see cref="SkipField" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (type != typeof(SkipField))
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(JsonSkipFieldDeserializerBuilderCase)} only supports {nameof(SkipField)}."));
            }

            // Utf8JsonReader.Skip() handles all JSON token types: it advances past simple values,
            // and recursively skips from StartObject/StartArray to their matching End* token.
            var skip = typeof(Utf8JsonReader).GetMethod(nameof(Utf8JsonReader.Skip))!;
            return JsonDeserializerBuilderCaseResult.FromExpression(Expression.Call(context.Reader, skip));
        }
    }
}
