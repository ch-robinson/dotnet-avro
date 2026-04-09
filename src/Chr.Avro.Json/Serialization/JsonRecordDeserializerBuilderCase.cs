using System.Diagnostics;
using Chr.Avro.Infrastructure;

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
    using Microsoft.CSharp.RuntimeBinder;

    using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class JsonRecordDeserializerBuilderCase : RecordDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        private static readonly ReflectionMembers KnownReflectionMembers = new();

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
                    ParameterExpression? parameter = null;
                    ParameterExpression? reference = null;
                    Expression? expression = null;

                    if (!context.RecursiveReferences.TryGetValue(schema, out var schemaIsRecursive))
                    {
                        RecursiveReferenceSearch.Collect(schema, context.RecursiveReferences);

                        schemaIsRecursive = context.RecursiveReferences[schema];
                    }

                    if (schemaIsRecursive)
                    {
                        parameter = Expression.Parameter(
                            Expression.GetDelegateType(context.Reader.Type.MakeByRefType(), type));

                        if (!context.References.TryGetValue((recordSchema, type), out reference))
                        {
                            context.References.Add((recordSchema, type), reference = parameter);
                        }
                    }

                    // then build/set the delegate if it hasn't been built yet:
                    if (parameter == reference)
                    {
                        var loop = Expression.Label();

                        if (!underlying.IsAssignableFrom(typeof(ExpandoObject)) &&
                            GetRecordConstructor(underlying, recordSchema) is ConstructorInfo constructor)
                        {
                            expression = DeserializeIntoConstructorParameters(context, underlying, recordSchema, constructor, loop);
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
                                        Expression.Property(context.Reader, KnownReflectionMembers.Utf8JsonReader.TokenType),
                                        Expression.Constant(JsonTokenType.StartObject)),
                                    Expression.Throw(
                                        Expression.Call(
                                            null,
                                            KnownReflectionMembers.JsonExceptionHelper.GetUnexpectedTokenException,
                                            context.Reader,
                                            Expression.Constant(new[] { JsonTokenType.StartObject })))),
                                Expression.Loop(
                                    Expression.Block(
                                        Expression.Call(context.Reader, KnownReflectionMembers.Utf8JsonReader.Read),
                                        Expression.IfThen(
                                            Expression.Equal(
                                                Expression.Property(context.Reader, KnownReflectionMembers.Utf8JsonReader.TokenType),
                                                Expression.Constant(JsonTokenType.EndObject)),
                                            Expression.Break(loop)),
                                        Expression.Switch(
                                            Expression.Call(context.Reader, KnownReflectionMembers.Utf8JsonReader.GetString),
                                            Expression.Throw(
                                                Expression.Call(
                                                    null,
                                                    KnownReflectionMembers.JsonExceptionHelper.GetUnknownRecordFieldException,
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
                                                            Expression.Call(context.Reader, KnownReflectionMembers.Utf8JsonReader.Read),
                                                            expression,
                                                            Expression.Empty()),
                                                        Expression.Constant(field.Name));
                                                })
                                                .ToArray())),
                                    loop),
                                Expression.ConvertChecked(value, type));
                        }

                        if (reference is not null)
                        {
                            expression = Expression.Lambda(
                                parameter.Type,
                                expression,
                                $"{recordSchema.Name} deserializer",
                                new[] { context.Reader });

                            context.Assignments.Add(reference, expression);
                        }
                    }

                    if (reference is not null)
                    {
                        expression = Expression.Invoke(reference, context.Reader);
                    }

                    Debug.Assert(expression != null, "Expression has not been built");
                    return JsonDeserializerBuilderCaseResult.FromExpression(expression);
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

        private static Type GetMemberType(MemberInfo match)
        {
            return match switch
            {
                FieldInfo fieldMatch => fieldMatch.FieldType,
                PropertyInfo propertyMatch => propertyMatch.PropertyType,
                MemberInfo unknown => throw new InvalidOperationException($"Record fields can only be mapped to fields and properties."),
            };
        }

        private Expression DeserializeIntoConstructorParameters(
            JsonDeserializerBuilderContext context,
            Type type,
            RecordSchema recordSchema,
            ConstructorInfo constructor,
            LabelTarget loop)
        {
            Expression expression;
            var ctorParameters = constructor.GetParameters();

            // Constructor is a match
            // All ctor parameters either has a matching record field, or a default value
            // But some record fields might not match any ctor parameter

            // Fields that have a match as a constructor parameter
            var matchedFields = recordSchema.Fields.Where(f => ctorParameters.Any(p => IsMatch(f, p.Name))).ToDictionary(f => f.Name);

            var members = type.GetMembers(MemberVisibility);

            var fieldToDeserializeToProperties = recordSchema.Fields
                .Where(f => !matchedFields.ContainsKey(f.Name))
                .Select(f => (field: f, member: members.FirstOrDefault(m => IsMatch(f, m))))
                .Where(x => x.member is not null)
                .ToDictionary(x => x.field.Name);

            var variables = new Dictionary<string, ParameterExpression>();

            // map fields to either a constructor parameter or a writable member
            var mappings = recordSchema.Fields
                .Select(field =>
                {
                    // there might not be a match for a particular field, in which case it will be deserialized and then ignored
                    var constructorParameter = ctorParameters.SingleOrDefault(parameter => IsMatch(field, parameter.Name));
                    MemberInfo matchedMember = null;
                    if (constructorParameter is null)
                    {
                        // No constructor parameter match for the field
                        // Can we find a member we can assign the value to?
                        if (fieldToDeserializeToProperties.TryGetValue(field.Name, out var memberMatch))
                        {
                            var memberType = GetMemberType(memberMatch.member);
                            var variable = Expression.Variable(memberType, memberMatch.member.Name)
                                ?? throw new InvalidOperationException($"Failed to create variable for {memberMatch.member}");
                            variables.Add(variable.Name, variable);
                            return (
                                Field: field,
                                Variable: (ParameterExpression?)variable,
                                ConstructorParameter: (ParameterInfo?)null,
                                Member: (MemberInfo?)memberMatch.member,
                                Assignment: (Expression)Expression.Block(
                                            Expression.Call(context.Reader, KnownReflectionMembers.Utf8JsonReader.Read),
                                            Expression.Assign(
                                                variable,
                                                DeserializerBuilder.BuildExpression(memberType, field.Type, context))));
                        }

                        // No match: we still emit an expression so that the field gets deserialised
                        return (
                            Field: field,
                            Variable: null,
                            ConstructorParameter: null,
                            Member: null,
                            Assignment: Expression.Block(
                                Expression.Call(context.Reader, KnownReflectionMembers.Utf8JsonReader.Read),
                                DeserializerBuilder.BuildExpression(typeof(object), field.Type, context)));
                    }

                    var parameter = Expression.Parameter(constructorParameter.ParameterType);
                    return (
                        Field: field,
                        Variable: parameter,
                        ConstructorParameter: constructorParameter,
                        Member: null,
                        Assignment: Expression.Block(
                            Expression.Call(context.Reader, KnownReflectionMembers.Utf8JsonReader.Read),
                            Expression.Assign(
                                parameter,
                                DeserializerBuilder.BuildExpression(constructorParameter.ParameterType, field.Type, context))));
                })
                .ToArray();

            var ctorParameterMatches = mappings
                .Where(x => x.ConstructorParameter != null)
                .ToDictionary(
                    x => x.ConstructorParameter!.Name!,
                    x => x.Variable ?? throw new InvalidOperationException($"Variable expected for deserialization of ctor parameter {x.ConstructorParameter!.Name}"));

            var memberMatches = mappings
                .Where(x => x.Member != null)
                .Select(x => (
                    Member: x.Member!.Name,
                    Variable: x.Variable ?? throw new InvalidOperationException($"Variable expected for deserialization of {x.Member}")))
                .ToArray();

            var value = Expression.Parameter(
                type.IsAssignableFrom(typeof(ExpandoObject))
                    ? typeof(ExpandoObject)
                    : type);

            var memberAssignments =
                memberMatches.Length == 0 ? (Expression)Expression.Empty()
                    : Expression.Block(
                        memberMatches.Select(m =>
                            Expression.Assign(
                                Expression.PropertyOrField(value, m.Member),
                                m.Variable)));

            expression = Expression.Block(
                mappings.Where(m => m.Variable != null).Select(m => m.Variable)
                    .Concat(new[] { value })!,
                Expression.IfThen(
                    Expression.NotEqual(
                        Expression.Property(context.Reader, KnownReflectionMembers.Utf8JsonReader.TokenType),
                        Expression.Constant(JsonTokenType.StartObject)),
                    Expression.Throw(
                        Expression.Call(
                            null,
                            KnownReflectionMembers.JsonExceptionHelper.GetUnexpectedTokenException,
                            context.Reader,
                            Expression.Constant(new[] { JsonTokenType.StartObject })))),
                Expression.Loop(
                    Expression.Block(
                        Expression.Call(context.Reader, KnownReflectionMembers.Utf8JsonReader.Read),
                        Expression.IfThen(
                            Expression.Equal(
                                Expression.Property(context.Reader, KnownReflectionMembers.Utf8JsonReader.TokenType),
                                Expression.Constant(JsonTokenType.EndObject)),
                            Expression.Break(loop)),
                        Expression.Switch(
                            Expression.Call(context.Reader, KnownReflectionMembers.Utf8JsonReader.GetString),
                            Expression.Throw(
                                Expression.Call(
                                    null,
                                    KnownReflectionMembers.JsonExceptionHelper.GetUnknownRecordFieldException,
                                    context.Reader)),
                            mappings
                                .Select(m =>
                                    Expression.SwitchCase(
                                        Expression.Block(m.Assignment, Expression.Empty()),
                                        Expression.Constant(m.Field.Name)))
                                .ToArray())),
                    loop),
                Expression.Assign(
                    value,
                    Expression.New(
                            constructor,
                            ctorParameters
                            .Select(parameter => ctorParameterMatches.TryGetValue(parameter.Name, out var match) ? (Expression)match
                            : Expression.Constant(parameter.DefaultValue, parameter.ParameterType)))),
                memberAssignments,
                value);

            return expression;
        }

        private class ReflectionMembers
        {
            public JsonReaderMembers Utf8JsonReader { get; } = new();

            public JsonExceptionHelperMembers JsonExceptionHelper { get; } = new();

            internal class JsonReaderMembers
            {
                public JsonReaderMembers()
                {
                    var type = typeof(Utf8JsonReader);
                    TokenType = type.GetProperty(nameof(Utf8JsonReader.TokenType))!;
                    Read = type.GetMethod(nameof(Utf8JsonReader.Read), Type.EmptyTypes)!;
                    GetString = type.GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes)!;
                }

                public PropertyInfo TokenType { get; }

                public MethodInfo Read { get; }

                public MethodInfo GetString { get; }
            }

            internal class JsonExceptionHelperMembers
            {
                public JsonExceptionHelperMembers()
                {
                    var type = typeof(JsonExceptionHelper);
                    GetUnexpectedTokenException = type
                        .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException))!;
                    GetUnknownRecordFieldException = type
                        .GetMethod(nameof(JsonExceptionHelper.GetUnknownRecordFieldException))!;
                }

                public MethodInfo GetUnexpectedTokenException { get; }

                public MethodInfo GetUnknownRecordFieldException { get; }
            }
        }
    }
}
