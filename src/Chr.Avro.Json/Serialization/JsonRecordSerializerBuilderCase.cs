namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class JsonRecordSerializerBuilderCase : RecordSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonRecordSerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build field serializers.
        /// </param>
        public JsonRecordSerializerBuilderCase(IJsonSerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "JSON serializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build field serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is a <see cref="RecordResolution" /> and <paramref name="schema" /> is a <see cref="RecordSchema" />;
        /// an unsuccessful <see cref="JsonSerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved <see cref="Type" /> does not have a matching member for each
        /// <see cref="RecordField" /> on <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    // since record serialization is potentially recursive, create a top-level
                    // reference:
                    var parameter = Expression.Parameter(
                        Expression.GetDelegateType(resolution.Type, context.Writer.Type, typeof(void)));

                    if (!context.References.TryGetValue((recordSchema, resolution.Type), out var reference))
                    {
                        context.References.Add((recordSchema, resolution.Type), reference = parameter);
                    }

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        var writeStartObject = typeof(Utf8JsonWriter)
                            .GetMethod(nameof(Utf8JsonWriter.WriteStartObject), Type.EmptyTypes);

                        var writePropertyName = typeof(Utf8JsonWriter)
                            .GetMethod(nameof(Utf8JsonWriter.WritePropertyName), new[] { typeof(string) });

                        var writeEndObject = typeof(Utf8JsonWriter)
                            .GetMethod(nameof(Utf8JsonWriter.WriteEndObject), Type.EmptyTypes);

                        var argument = Expression.Variable(resolution.Type);
                        var writes = new List<Expression>
                        {
                            Expression.Call(context.Writer, writeStartObject),
                        };

                        foreach (var field in recordSchema.Fields)
                        {
                            var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));

                            if (match == null)
                            {
                                throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type} does not have a field or property that matches the {field.Name} field on {recordSchema.Name}.");
                            }

                            writes.Add(Expression.Call(context.Writer, writePropertyName, Expression.Constant(field.Name)));
                            writes.Add(SerializerBuilder.BuildExpression(Expression.PropertyOrField(argument, match.Member.Name), field.Type, context));
                        }

                        writes.Add(Expression.Call(context.Writer, writeEndObject));

                        var expression = Expression.Lambda(
                            parameter.Type,
                            Expression.Block(writes),
                            $"{recordSchema.Name} serializer",
                            new[] { argument, context.Writer });

                        context.Assignments.Add(reference, expression);
                    }

                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Invoke(reference, value, context.Writer));
                }
                else
                {
                    return JsonSerializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(JsonRecordSerializerBuilderCase)} can only be applied to {nameof(RecordResolution)}s."));
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonRecordSerializerBuilderCase)} can only be applied to {nameof(RecordSchema)}s."));
            }
        }
    }
}
