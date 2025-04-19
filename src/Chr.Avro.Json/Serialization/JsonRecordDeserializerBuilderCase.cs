namespace Chr.Avro.Serialization
{
    using System;
    using System.Dynamic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Microsoft.CSharp.RuntimeBinder;

    using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class JsonRecordDeserializerBuilderCase : RecordDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonRecordDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build field deserializers.
        /// </param>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        public JsonRecordDeserializerBuilderCase(
            IJsonDeserializerBuilder deserializerBuilder,
            BindingFlags memberVisibility)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "JSON deserializer builder cannot be null.");
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build field deserializers.
        /// </summary>
        public IJsonDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Gets the binding flags used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is not an array or primitive type and <paramref name="schema" /> is a <see cref="RecordSchema" />;
        /// an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is RecordSchema recordSchema)
            {
                var underlying = Nullable.GetUnderlyingType(type) ?? type;

                if (!underlying.IsArray && !underlying.IsPrimitive)
                {
                    // since record deserialization is potentially recursive, create a top-level
                    // reference:
                    var parameter = Expression.Parameter(
                        Expression.GetDelegateType(context.Reader.Type.MakeByRefType(), type));

                    if (!context.References.TryGetValue((recordSchema, type), out var reference))
                    {
                        context.References.Add((recordSchema, type), reference = parameter);
                    }

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        Expression expression;

                        var loop = Expression.Label();

                        var tokenType = typeof(Utf8JsonReader)
                            .GetProperty(nameof(Utf8JsonReader.TokenType));

                        var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                            .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                        var read = typeof(Utf8JsonReader)
                            .GetMethod(nameof(Utf8JsonReader.Read), Type.EmptyTypes);

                        var getString = typeof(Utf8JsonReader)
                            .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                        var getUnknownRecordFieldException = typeof(JsonExceptionHelper)
                            .GetMethod(nameof(JsonExceptionHelper.GetUnknownRecordFieldException));

                        if (GetRecordConstructor(underlying, recordSchema) is ConstructorInfo constructor)
                        {
                            var parameters = constructor.GetParameters();

                            // map constructor parameters to fields:
                            var mapping = recordSchema.Fields
                                .Select(field =>
                                {
                                    // there will be a match or we wouldn’t have made it this far:
                                    var match = parameters.Single(parameter => IsMatch(field, parameter.Name));
                                    var parameter = Expression.Parameter(match.ParameterType);

                                    return (
                                        Field: field,
                                        Match: match,
                                        Parameter: parameter,
                                        Assignment: (Expression)Expression.Block(
                                            Expression.Call(context.Reader, read),
                                            Expression.Assign(
                                                parameter,
                                                DeserializerBuilder.BuildExpression(match.ParameterType, field.Type, context))));
                                })
                                .ToDictionary(r => r.Match);

                            expression = Expression.Block(
                                mapping
                                    .Select(d => d.Value.Parameter),
                                Expression.IfThen(
                                    Expression.NotEqual(
                                        Expression.Property(context.Reader, tokenType),
                                        Expression.Constant(JsonTokenType.StartObject)),
                                    Expression.Throw(
                                        Expression.Call(
                                            null,
                                            getUnexpectedTokenException,
                                            context.Reader,
                                            Expression.Constant(new[] { JsonTokenType.StartObject })))),
                                Expression.Loop(
                                    Expression.Block(
                                        Expression.Call(context.Reader, read),
                                        Expression.IfThen(
                                            Expression.Equal(
                                                Expression.Property(context.Reader, tokenType),
                                                Expression.Constant(JsonTokenType.EndObject)),
                                            Expression.Break(loop)),
                                        Expression.Switch(
                                            Expression.Call(context.Reader, getString),
                                            Expression.Throw(
                                                Expression.Call(
                                                    null,
                                                    getUnknownRecordFieldException,
                                                    context.Reader)),
                                            mapping
                                                .Select(pair =>
                                                    Expression.SwitchCase(
                                                        Expression.Block(pair.Value.Assignment, Expression.Empty()),
                                                        Expression.Constant(pair.Value.Field.Name)))
                                                .ToArray())),
                                    loop),
                                Expression.New(
                                    constructor,
                                    parameters
                                        .Select(parameter => mapping.ContainsKey(parameter)
                                            ? (Expression)mapping[parameter].Parameter
                                            : Expression.Constant(parameter.DefaultValue, parameter.ParameterType))));
                        }
                        else
                        {
                            // support dynamic deserialization:
                            var value = Expression.Parameter(
                                underlying.IsAssignableFrom(typeof(ExpandoObject))
                                    ? typeof(ExpandoObject)
                                    : underlying);

                            expression = Expression.Block(
                                new[] { value },
                                Expression.Assign(value, Expression.New(value.Type)),
                                Expression.IfThen(
                                    Expression.NotEqual(
                                        Expression.Property(context.Reader, tokenType),
                                        Expression.Constant(JsonTokenType.StartObject)),
                                    Expression.Throw(
                                        Expression.Call(
                                            null,
                                            getUnexpectedTokenException,
                                            context.Reader,
                                            Expression.Constant(new[] { JsonTokenType.StartObject })))),
                                Expression.Loop(
                                    Expression.Block(
                                        Expression.Call(context.Reader, read),
                                        Expression.IfThen(
                                            Expression.Equal(
                                                Expression.Property(context.Reader, tokenType),
                                                Expression.Constant(JsonTokenType.EndObject)),
                                            Expression.Break(loop)),
                                        Expression.Switch(
                                            Expression.Call(context.Reader, getString),
                                            Expression.Throw(
                                                Expression.Call(
                                                    null,
                                                    getUnknownRecordFieldException,
                                                    context.Reader)),
                                            recordSchema.Fields
                                                .Select(field =>
                                                {
                                                    var match = GetMatch(field, underlying, MemberVisibility);

                                                    Expression expression;

                                                    if (match == null)
                                                    {
                                                        // always deserialize fields to advance the reader:
                                                        expression = DeserializerBuilder.BuildExpression(typeof(object), field.Type, context);

                                                        // fall back to a dynamic setter if the value supports it:
                                                        if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(value.Type))
                                                        {
                                                            var flags = CSharpBinderFlags.None;
                                                            var infos = new[] { CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null) };
                                                            var binder = Binder.SetMember(flags, field.Name, value.Type, infos);
                                                            expression = Expression.Dynamic(binder, typeof(void), value, expression);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        Expression inner;

                                                        try
                                                        {
                                                            inner = DeserializerBuilder.BuildExpression(
                                                                match switch
                                                                {
                                                                    FieldInfo fieldMatch => fieldMatch.FieldType,
                                                                    PropertyInfo propertyMatch => propertyMatch.PropertyType,
                                                                    MemberInfo unknown => throw new InvalidOperationException($"Record fields can only be mapped to fields and properties."),
                                                                },
                                                                field.Type,
                                                                context);
                                                        }
                                                        catch (Exception exception)
                                                        {
                                                            throw new UnsupportedTypeException(type, $"The {match.Name} member on {type} could not be mapped to the {field.Name} field on {recordSchema.FullName}.", exception);
                                                        }

                                                        expression = Expression.Assign(
                                                            Expression.PropertyOrField(value, match.Name),
                                                            inner);
                                                    }

                                                    return Expression.SwitchCase(
                                                        Expression.Block(
                                                            Expression.Call(context.Reader, read),
                                                            expression,
                                                            Expression.Empty()),
                                                        Expression.Constant(field.Name));
                                                })
                                                .ToArray())),
                                    loop),
                                Expression.ConvertChecked(value, type));
                        }

                        expression = Expression.Lambda(
                            parameter.Type,
                            expression,
                            $"{recordSchema.Name} deserializer",
                            new[] { context.Reader });

                        context.Assignments.Add(reference, expression);
                    }

                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        Expression.Invoke(reference, context.Reader));
                }
                else
                {
                    return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(JsonRecordDeserializerBuilderCase)} cannot be applied to array or primitive types."));
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonRecordDeserializerBuilderCase)} can only be applied to {nameof(RecordSchema)}s."));
            }
        }
    }
}
