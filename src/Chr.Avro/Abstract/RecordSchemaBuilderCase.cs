namespace Chr.Avro.Abstract
{
    using System;
    using System.ComponentModel;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text.RegularExpressions;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches any non-array or non-primitive
    /// <see cref="Type" />.
    /// </summary>
    public class RecordSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        private readonly NullabilityInfoContext nullabilityContext;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <param name="nullableReferenceTypeBehavior">
        /// The behavior to use to determine nullability of reference types.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to build schemas for field types.
        /// </param>
        public RecordSchemaBuilderCase(
            BindingFlags memberVisibility,
            NullableReferenceTypeBehavior nullableReferenceTypeBehavior,
            ISchemaBuilder schemaBuilder)
        {
            MemberVisibility = memberVisibility;
            NullableReferenceTypeBehavior = nullableReferenceTypeBehavior;
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder cannot be null.");

            nullabilityContext = new NullabilityInfoContext();
        }

        /// <summary>
        /// Gets the binding flags used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Gets the behavior used to determine nullability of reference types.
        /// </summary>
        public NullableReferenceTypeBehavior NullableReferenceTypeBehavior { get; }

        /// <summary>
        /// Gets the schema builder instance that will be used to build schemas for field types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="RecordSchema" />
        /// if <paramref name="type" /> is not an array or primitve type; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (!type.IsArray && !type.IsPrimitive)
            {
                // defer setting the field schemas so the record schema can be cached:
                var recordSchema = new RecordSchema(GetSchemaName(type))
                {
                    Namespace = GetSchemaNamespace(type),
                };

                Schema schema = recordSchema;

                if (!type.IsValueType && NullableReferenceTypeBehavior == NullableReferenceTypeBehavior.All)
                {
                    schema = MakeNullableSchema(schema);
                }

                try
                {
                    context.Schemas.Add(type, schema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                foreach (var member in type.GetMembers(MemberVisibility)
                    .OrderBy(member => type.HasAttribute<DataContractAttribute>()
                        ? member.GetAttribute<DataMemberAttribute>()?.Order ?? 0
                        : default)
                    .ThenBy(member => member.Name))
                {
                    if (!type.HasAttribute<DataContractAttribute>()
                        && member.HasAttribute<NonSerializedAttribute>())
                    {
                        continue;
                    }

                    if (type.HasAttribute<DataContractAttribute>()
                        && !member.HasAttribute<DataMemberAttribute>())
                    {
                        continue;
                    }

                    var memberType = member switch
                    {
                        FieldInfo fieldInfo => fieldInfo.FieldType,
                        PropertyInfo propertyInfo => propertyInfo.PropertyType,
                        _ => default,
                    };

                    if (memberType == null)
                    {
                        continue;
                    }

                    var field = new RecordField(GetFieldName(member), SchemaBuilder.BuildSchema(memberType, context));

                    if (NullableReferenceTypeBehavior == NullableReferenceTypeBehavior.Annotated)
                    {
                        var nullabilityInfo = member switch
                        {
                            FieldInfo fieldInfo => nullabilityContext.Create(fieldInfo),
                            PropertyInfo propertyInfo => nullabilityContext.Create(propertyInfo),
                            _ => default,
                        };

                        if (nullabilityInfo != null)
                        {
                            field.Type = ApplyNullabilityInfo(field.Type, nullabilityInfo);
                        }
                    }

                    if (member.GetAttribute<DefaultValueAttribute>() is DefaultValueAttribute defaultAttribute)
                    {
                        field.Default = new ObjectDefaultValue<object>(defaultAttribute.Value, field.Type);
                    }

                    recordSchema.Fields.Add(field);
                }

                return SchemaBuilderCaseResult.FromSchema(schema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(RecordSchemaBuilderCase)} cannot be applied to array or primitive types."));
            }
        }

        /// <summary>
        /// Derives a field name from a <see cref="MemberInfo" />.
        /// </summary>
        /// <param name="member">
        /// A member to derive the name from.
        /// </param>
        /// <returns>
        /// A field name that conforms to the Avro naming rules.
        /// </returns>
        protected virtual string GetFieldName(MemberInfo member)
        {
            if (member.DeclaringType.HasAttribute<DataContractAttribute>()
                && member.GetAttribute<DataMemberAttribute>() is DataMemberAttribute memberAttribute
                && !string.IsNullOrEmpty(memberAttribute.Name))
            {
                return memberAttribute.Name;
            }
            else
            {
                return member.Name;
            }
        }

        /// <summary>
        /// Derives a schema name from a <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A type to derive the name from.
        /// </param>
        /// <returns>
        /// An unqualified schema name that conforms to the Avro naming rules.
        /// </returns>
        protected virtual string GetSchemaName(Type type)
        {
            if (type.GetAttribute<DataContractAttribute>() is DataContractAttribute contractAttribute
                && !string.IsNullOrEmpty(contractAttribute.Name))
            {
                return contractAttribute.Name;
            }
            else
            {
                var name = Regex.Replace(type.Name, @"`\d+$", string.Empty);

                foreach (var parameter in type.GetGenericArguments())
                {
                    name += $"_{GetSchemaName(parameter)}";
                }

                return name;
            }
        }

        /// <summary>
        /// Derives a schema namespace from a <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A type to derive the namespace from.
        /// </param>
        /// <returns>
        /// An schema namespace that conforms to the Avro naming rules.
        /// </returns>
        protected virtual string? GetSchemaNamespace(Type type)
        {
            if (type.GetAttribute<DataContractAttribute>() is DataContractAttribute contractAttribute
                && !string.IsNullOrEmpty(contractAttribute.Namespace))
            {
                return contractAttribute.Namespace;
            }
            else
            {
                return string.IsNullOrEmpty(type.Namespace) ? null : type.Namespace;
            }
        }

        /// <summary>
        /// Makes a schema and any of its children nullable based on a type member's nullability.
        /// </summary>
        /// <param name="schema">
        /// A <see cref="Schema" /> object to apply nullability info to.
        /// </param>
        /// <param name="nullabilityInfo">
        /// A <see cref="NullabilityInfo" /> object for the member that <paramref name="schema" />
        /// represents.
        /// </param>
        private static Schema ApplyNullabilityInfo(Schema schema, NullabilityInfo nullabilityInfo)
        {
            if (schema is ArraySchema arraySchema)
            {
                if (nullabilityInfo.ElementType != null)
                {
                    // if the type is an array, this is easy; recurse with the element type info:
                    schema = new ArraySchema(ApplyNullabilityInfo(
                        arraySchema.Item,
                        nullabilityInfo.ElementType))
                    {
                        LogicalType = schema.LogicalType,
                    };
                }
                else
                {
                    // otherwise, if the type is generic, try to map one of its type arguments to
                    // the IEnumerable<T> type argument and recurse with that argument type info:
                    var genericType = nullabilityInfo.Type
                        .GetGenericTypeDefinition();

                    if (genericType.GetEnumerableType() is Type genericItemType)
                    {
                        var infoIndex = genericType
                            .GetGenericArguments()
                            .ToList()
                            .FindIndex(candidate => candidate == genericItemType);

                        if (infoIndex >= 0)
                        {
                            schema = new ArraySchema(ApplyNullabilityInfo(
                                arraySchema.Item,
                                nullabilityInfo.GenericTypeArguments[infoIndex]));
                        }
                    }
                }
            }
            else if (schema is MapSchema mapSchema)
            {
                // if the type is generic, use the same trick as for IEnumerable<T>:
                var genericType = nullabilityInfo.Type
                    .GetGenericTypeDefinition();

                if (genericType.GetDictionaryTypes() is (_, Type genericValueType))
                {
                    var infoIndex = genericType
                        .GetGenericArguments()
                        .ToList()
                        .FindIndex(candidate => candidate == genericValueType);

                    if (infoIndex >= 0)
                    {
                        schema = new MapSchema(ApplyNullabilityInfo(
                            mapSchema.Value,
                            nullabilityInfo.GenericTypeArguments[infoIndex]));
                    }
                }
            }

            // check the top level last (this also handles UnionSchema, which is the only other
            // schema type that can have children):
            if (nullabilityInfo.ReadState == NullabilityState.Nullable)
            {
                schema = MakeNullableSchema(schema);
            }

            return schema;
        }

        /// <summary>
        /// Ensures that a schema is nullable (a <see cref="UnionSchema" /> containing a
        /// <see cref="NullSchema" />).
        /// </summary>
        /// <param name="schema">
        /// A schema to make nullable if not already.
        /// </param>
        /// <returns>
        /// A <see cref="UnionSchema" /> containing a <see cref="NullSchema" />. If
        /// <paramref name="schema" /> is already a <see cref="UnionSchema" /> containing a
        /// <see cref="NullSchema" />, it will be returned as is. If <paramref name="schema" />
        /// is a <see cref="UnionSchema" /> but does not contain a <see cref="NullSchema" />, a new
        /// <see cref="UnionSchema" /> will be returned with a new <see cref="NullSchema" /> as the
        /// first member. If <paramref name="schema" /> is not a <see cref="UnionSchema" />, a new
        /// <see cref="UnionSchema" /> will be returned comprising a <see cref="NullSchema" /> and
        /// <paramref name="schema" />.
        /// </returns>
        private static Schema MakeNullableSchema(Schema schema)
        {
            if (schema is UnionSchema unionSchema)
            {
                // if a null schema is already present, all good:
                if (unionSchema.Schemas.Any(schema => schema is NullSchema))
                {
                    return schema;
                }
                else
                {
                    // ordinarily, null would come first in a union, but since default values for
                    // record fields correspond to the first schema in the union, we append to
                    // avoid invalidating any default values:
                    return new UnionSchema(unionSchema.Schemas.Concat(new[] { new NullSchema() }))
                    {
                        LogicalType = unionSchema.LogicalType,
                    };
                }
            }
            else
            {
                return new UnionSchema(new[] { new NullSchema(), schema });
            }
        }
    }
}
