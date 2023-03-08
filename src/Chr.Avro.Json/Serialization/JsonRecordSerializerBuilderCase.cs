namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class JsonRecordSerializerBuilderCase : RecordSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonRecordSerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build field serializers.
        /// </param>
        public JsonRecordSerializerBuilderCase(
            BindingFlags memberVisibility,
            IJsonSerializerBuilder serializerBuilder)
        {
            MemberVisibility = memberVisibility;
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "JSON serializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the binding flags used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build field serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="type" />
        /// is not an array or primitive type and <paramref name="schema" /> is a <see cref="RecordSchema" />;
        /// an unsuccessful <see cref="JsonSerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> does not have a matching member for each
        /// <see cref="RecordField" /> on <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is RecordSchema recordSchema)
            {
                if (!type.IsArray && !type.IsPrimitive)
                {
                    // since record serialization is potentially recursive, create a top-level
                    // reference:
                    var parameter = Expression.Parameter(
                        Expression.GetDelegateType(type, context.Writer.Type, typeof(void)));

                    if (!context.References.TryGetValue((recordSchema, type), out var reference))
                    {
                        context.References.Add((recordSchema, type), reference = parameter);
                    }

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        var members = type.GetMembers(MemberVisibility);

                        var writeStartObject = typeof(Utf8JsonWriter)
                            .GetMethod(nameof(Utf8JsonWriter.WriteStartObject), Type.EmptyTypes);

                        var writePropertyName = typeof(Utf8JsonWriter)
                            .GetMethod(nameof(Utf8JsonWriter.WritePropertyName), new[] { typeof(string) });

                        var writeEndObject = typeof(Utf8JsonWriter)
                            .GetMethod(nameof(Utf8JsonWriter.WriteEndObject), Type.EmptyTypes);

                        var argument = Expression.Variable(type);
                        var writes = new List<Expression>
                        {
                            Expression.Call(context.Writer, writeStartObject),
                        };

                        foreach (var field in recordSchema.Fields)
                        {
                            var match = members.SingleOrDefault(member => IsMatch(field, member));

                            Expression inner;

                            if (match == null)
                            {
                                // if the type could be dynamic, attempt to use a dynamic getter:
                                if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(type) || type == typeof(object))
                                {
                                    inner = this.BuildDynamicGet(value, field.Name);
                                }
                                else
                                {
                                    if (field.Default is not null)
                                    {
                                        inner = Expression.Constant(field.Default.ToObject<dynamic>());
                                    }
                                    else
                                    {
                                        throw new UnsupportedTypeException(type, $"{type} does not have a field or property that matches the {field.Name} field on {recordSchema.FullName}.");
                                    }
                                }
                            }
                            else
                            {
                                inner = Expression.PropertyOrField(argument, match.Name);
                            }

                            try
                            {
                                writes.Add(Expression.Call(context.Writer, writePropertyName, Expression.Constant(field.Name)));
                                writes.Add(SerializerBuilder.BuildExpression(inner, field.Type, context));
                            }
                            catch (Exception exception)
                            {
                                throw new UnsupportedTypeException(type, $"{(match is null ? "A" : $"The {match.Name}")} member on {type} could not be mapped to the {field.Name} field on {recordSchema.FullName}.", exception);
                            }
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
                    return JsonSerializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(JsonRecordSerializerBuilderCase)} cannot be applied to array or primitive types."));
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonRecordSerializerBuilderCase)} can only be applied to {nameof(RecordSchema)}s."));
            }
        }
    }
}
