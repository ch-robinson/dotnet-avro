using System.Collections.Generic;

namespace Chr.Avro.Abstract
{
    using System;
    using System.ComponentModel;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="Enum" /> types.
    /// </summary>
    public class EnumSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="EnumSchemaBuilderCase" /> class.
        /// </summary>
        /// <param name="enumBehavior">
        /// A value value indicating whether the case should build enum schemas or integral schemas.
        /// </param>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to build schemas for underlying integral
        /// types.
        /// </param>
        public EnumSchemaBuilderCase(EnumBehavior enumBehavior, ISchemaBuilder schemaBuilder)
        {
            EnumBehavior = enumBehavior;
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(SchemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Gets a value indicating whether the case should build enum schemas or integral schemas.
        /// </summary>
        public EnumBehavior EnumBehavior { get; }

        /// <summary>
        /// Gets the schema builder instance that will be used to build schemas for underlying
        /// integral types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Builds an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="EnumSchema" />
        /// if <paramref name="type" /> is an <see cref="Enum" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type.IsEnum)
            {
                Schema schema;

                if (type.GetAttribute<FlagsAttribute>() is not null || EnumBehavior == EnumBehavior.Integral)
                {
                    schema = SchemaBuilder.BuildSchema(type.GetEnumUnderlyingType(), context);
                }
                else if (EnumBehavior == EnumBehavior.Nominal)
                {
                    schema = new StringSchema();
                }
                else
                {
                    var enumMembers = type.GetEnumMembers().ToList();
                    var enumSchema = new EnumSchema(GetSchemaName(type))
                    {
                        Namespace = GetSchemaNamespace(type),
                        Default = GetDefaultValue(type, enumMembers),
                        Documentation = type.GetAttribute<DescriptionAttribute>()?.Description
                    };

                    foreach (var member in enumMembers
                        .OrderBy(field => Enum.Parse(type, field.Name))
                        .ThenBy(field => field.Name))
                    {
                        enumSchema.Symbols.Add(GetSymbol(member));
                    }

                    schema = enumSchema;
                }

                try
                {
                    context.Schemas.Add(type, schema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(schema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(EnumSchemaBuilderCase)} can only be applied to {typeof(Enum)} types."));
            }
        }

        private string? GetDefaultValue(Type type, IEnumerable<MemberInfo> enumMembers)
        {
            var enumDefaultValue = type.GetAttribute<DefaultValueAttribute>()?.Value;
            if (enumDefaultValue is null)
                return null;
            var matchedMember = enumMembers
                .Single(member => member.Name == Enum.GetName(type, enumDefaultValue));
            return GetSymbol(matchedMember);
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
            var dataContractAttribute = type.GetAttribute<DataContractAttribute>();

            if (dataContractAttribute is not null && !string.IsNullOrEmpty(dataContractAttribute.Name))
            {
                return dataContractAttribute.Name;
            }
            else
            {
                return type.Name;
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
        /// Derives an enum symbol from a <see cref="MemberInfo" />.
        /// </summary>
        /// <param name="member">
        /// A member to derive the name from.
        /// </param>
        /// <returns>
        /// A symbol that conforms to the Avro naming rules.
        /// </returns>
        protected virtual string GetSymbol(MemberInfo member)
        {
            return member.GetEnumMemberName();
        }
    }
}
