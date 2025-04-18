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
    using Chr.Avro.Abstract;
    using Microsoft.CSharp.RuntimeBinder;
    using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class BinaryRecordDeserializerBuilderCase : RecordDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build field deserializers.
        /// </param>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        public BinaryRecordDeserializerBuilderCase(
            IBinaryDeserializerBuilder deserializerBuilder,
            BindingFlags memberVisibility)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build field deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Gets the binding flags used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is not an array or primitive type and <paramref name="schema" /> is a <see cref="RecordSchema" />;
        /// an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
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
                        if (!underlying.IsAssignableFrom(typeof(ExpandoObject)) &&
                            GetRecordConstructor(underlying, recordSchema) is ConstructorInfo constructor)
                        {
                            expression = DeserializeUsingConstructor(context, underlying, recordSchema, constructor);
                        }
                        else
                        {
                            // We couldn't find a constructor to use
                            // Simply use the default constructor, and deserialize into public members

                            // support dynamic deserialization:
                            var deserializedType = underlying.IsAssignableFrom(typeof(ExpandoObject))
                                    ? typeof(ExpandoObject)
                                    : underlying;

                            // Check we do have a default constructor
                            if (deserializedType.GetConstructor(Array.Empty<Type>()) is null)
                            {
                                var error = $"Unable to map schema {schema} to type {underlying} as it doesn't have " +
                                    "a default constructor, and no compatible constructor could be found.";
                                throw new UnsupportedTypeException(underlying, error);
                            }

                            var value = Expression.Parameter(deserializedType);

                            Expression ctorExpression = Expression.Assign(value, Expression.New(value.Type));

                            expression = Expression.Block(
                                new[] { value },
                                new[] { ctorExpression }
                                    .Concat(DeserializeToProperties(type, context, value, recordSchema.Fields, recordSchema))
                                    .Concat(new[] { Expression.ConvertChecked(value, type) }));
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
                    return BinaryDeserializerBuilderCaseResult.FromExpression(expression);
                }
                else
                {
                    return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryRecordDeserializerBuilderCase)} cannot be applied to array or primitive types."));
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryRecordDeserializerBuilderCase)} can only be applied to {nameof(RecordSchema)}s."));
            }
        }

        private static Type GetMemberType(MemberInfo match) => match switch
        {
            FieldInfo fieldMatch => fieldMatch.FieldType,
            PropertyInfo propertyMatch => propertyMatch.PropertyType,
            _ => throw new InvalidOperationException("Record fields can only be mapped to fields and properties."),
        };

        private IEnumerable<Expression> DeserializeToProperties(Type type, BinaryDeserializerBuilderContext context, ParameterExpression value, IEnumerable<RecordField> fields, RecordSchema recordSchema)
        {
            var ret = fields.Select(field =>
            {
                var match = GetMatch(field, type, MemberVisibility);

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
                            GetMemberType(match),
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

                return expression;
            });

            return ret;
        }

        private Expression DeserializeUsingConstructor(
            BinaryDeserializerBuilderContext context,
            Type type,
            RecordSchema recordSchema,
            ConstructorInfo constructor)
        {
            var ctorParameters = constructor.GetParameters();

            // Constructor is a match
            // All ctor parameters either have a matching record field, or a default value
            // But some record fields might not match any ctor parameter

            // Fields that have a match as a constructor parameter
            var fieldsMatchedToConstructorParam = recordSchema.Fields
                .Where(f => ctorParameters.Any(p => IsMatch(f, p.Name!)))
                .ToDictionary(f => f.Name);

            var members = type.GetMembers(MemberVisibility);

            var fieldToDeserializeToProperties = recordSchema.Fields
                .Where(f => !fieldsMatchedToConstructorParam.ContainsKey(f.Name))
                .Select(f => (field: f, member: members.FirstOrDefault(m => IsMatch(f, m))))
                .Where(x => x.member is not null)
                .ToDictionary(x => x.field.Name);

            var variables = new Dictionary<string, ParameterExpression>();

            // Map fields to either a constructor parameter or a writable member
            var mappings = recordSchema.Fields
                .Select(field =>
                {
                    // There might not be a match for a particular field, in which case it will be deserialized and then ignored
                    var constructorParameter = ctorParameters.SingleOrDefault(parameter => IsMatch(field, parameter.Name!));

                    if (constructorParameter is null)
                    {
                        // No constructor parameter match for the field
                        // Can we find a member we can assign the value to?
                        if (fieldToDeserializeToProperties.TryGetValue(field.Name, out var memberMatch))
                        {
                            var memberType = GetMemberType(memberMatch.member!);
                            var variable = Expression.Variable(memberType, memberMatch.member!.Name)
                                ?? throw new InvalidOperationException($"Failed to create variable for {memberMatch.member}");
                            variables.Add(variable.Name!, variable);
                            return (
                                IsMatch: true,
                                Variable: (ParameterExpression?)variable,
                                ConstructorParameter: (ParameterInfo?)null,
                                Member: (MemberInfo?)memberMatch.member,
                                Assignment: (Expression)Expression.Assign(variable, DeserializerBuilder.BuildExpression(memberType, field.Type, context)));
                        }

                        // No match: we still emit an expression so that the field gets deserialized
                        return (IsMatch: false, Variable: null, ConstructorParameter: null, Member: null, Assignment: DeserializerBuilder.BuildExpression(typeof(object), field.Type, context));
                    }

                    var parameter = Expression.Parameter(constructorParameter.ParameterType);
                    return (
                        IsMatch: true,
                        Variable: parameter,
                        ConstructorParameter: constructorParameter,
                        Member: null,
                        Assignment: Expression.Assign(
                            parameter,
                            DeserializerBuilder.BuildExpression(constructorParameter.ParameterType, field.Type, context)));
                })
                .ToArray();

            var ctorParameterMatches = mappings
                .Where(x => x.ConstructorParameter != null)
                .ToDictionary(
                    x => x.ConstructorParameter!.Name!,
                    x => x.Variable ?? throw new InvalidOperationException($"Variable expected for deserialization of ctor parameter {x.ConstructorParameter!.Name}"));

            var memberMatches = mappings
                .Where(x => x.Member is not null)
                .Select(x => (
                    Member: x.Member!.Name,
                    Variable: x.Variable ?? throw new InvalidOperationException($"Variable expected for deserialization of {x.Member}")))
                .ToArray();

            var value = Expression.Parameter(type);

            var expression = Expression.Block(
                mappings.Where(m => m.Variable != null).Select(m => m.Variable!)
                    .Concat(new[] { value }),
                mappings.Select(d => d.Assignment)
                    .Concat(new[]
                    {
                        Expression.Assign(
                            value,
                            Expression.New(
                                constructor,
                                ctorParameters
                                    .Select(parameter => ctorParameterMatches.TryGetValue(parameter.Name!, out var match) ? (Expression)match
                                        : Expression.Constant(parameter.DefaultValue, parameter.ParameterType)))),
                    })
                    .Concat(memberMatches.Select(m =>
                        Expression.Assign(
                            Expression.PropertyOrField(value, m.Member),
                            m.Variable)))
                    .Concat(new[] { value }));

            return expression;
        }
    }
}
