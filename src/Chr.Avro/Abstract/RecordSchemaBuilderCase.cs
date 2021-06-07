namespace Chr.Avro.Abstract
{
    using System;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches any non-array or non-primitive
    /// <see cref="Type" />.
    /// </summary>
    public class RecordSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
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
                var recordSchema = new RecordSchema(type.Name)
                {
                    Namespace = string.IsNullOrEmpty(type.Namespace) ? null : type.Namespace,
                };

                Schema schema = recordSchema;

                if (!type.IsValueType && NullableReferenceTypeBehavior == NullableReferenceTypeBehavior.All)
                {
                    schema = new UnionSchema(new[] { new NullSchema(), schema });
                }

                try
                {
                    context.Schemas.Add(type, schema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                foreach (var member in type.GetMembers(MemberVisibility).OrderBy(member => member.Name))
                {
                    var memberType = member switch
                    {
                        FieldInfo fieldMember => fieldMember.FieldType,
                        PropertyInfo propertyMember => propertyMember.PropertyType,
                        _ => null
                    };

                    if (memberType == null)
                    {
                        continue;
                    }

                    recordSchema.Fields.Add(new RecordField(member.Name, SchemaBuilder.BuildSchema(memberType, context)));
                }

                return SchemaBuilderCaseResult.FromSchema(schema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(RecordSchemaBuilderCase)} cannot be applied to array or primitive types."));
            }
        }
    }
}
