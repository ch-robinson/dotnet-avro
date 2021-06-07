namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="BooleanSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonBooleanSerializerBuilderCase : BooleanSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a boolean serializer for a type-schema pair.
        /// </summary>
        /// <returns>
        /// A successful result if the schema is a <see cref="BooleanSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="bool" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is BooleanSchema booleanSchema)
            {
                var writeBoolean = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteBooleanValue), new[] { typeof(bool) });

                try
                {
                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeBoolean, BuildConversion(value, typeof(bool))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {booleanSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonBooleanSerializerBuilderCase)} can only be applied to {nameof(BooleanSchema)}s."));
            }
        }
    }
}
