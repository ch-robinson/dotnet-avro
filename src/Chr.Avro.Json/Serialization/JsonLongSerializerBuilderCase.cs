namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="LongSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonLongSerializerBuilderCase : LongSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a long serializer for a type-schema pair.
        /// </summary>
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is an <see cref="LongSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="long" />.
        /// </exception>
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is LongSchema longSchema)
            {
                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(long) });

                try
                {
                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeNumber, BuildConversion(value, typeof(long))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(resolution.Type, $"Failed to map {longSchema} to {resolution.Type}.", exception);
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonLongSerializerBuilderCase)} can only be applied to {nameof(LongSchema)}s."));
            }
        }
    }
}
