namespace Chr.Avro.Resolution
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="Enum" />s, taking
    /// <see cref="System.Runtime.Serialization" /> attributes into account.
    /// </summary>
    public class DataContractObjectTypeResolverCase : ObjectTypeResolverCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataContractObjectTypeResolverCase" />
        /// class.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        public DataContractObjectTypeResolverCase(BindingFlags memberVisibility)
        : base(memberVisibility)
        {
        }

        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="RecordResolution" />
        /// if <paramref name="type" /> is not an array or primitive type; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise. If <paramref name="type" /> has <see cref="DataContractAttribute" />, the
        /// <see cref="RecordResolution" /> will only include members that have
        /// <see cref="DataMemberAttribute" />. If <paramref name="type" /> does not have
        /// <see cref="DataContractAttribute" />, the <see cref="RecordResolution" /> will include
        /// all members that do not have <see cref="NonSerializedAttribute" />.
        /// </returns>
        /// <inheritdoc />
        public override TypeResolverCaseResult ResolveType(Type type)
        {
            var result = base.ResolveType(type);

            if (result.TypeResolution is RecordResolution recordResolution)
            {
                var contractAttribute = type.GetAttribute<DataContractAttribute>();

                if (contractAttribute == null)
                {
                    recordResolution.Fields = recordResolution.Fields
                        .Where(field => field.Member.GetAttribute<NonSerializedAttribute>() == null)
                        .ToList();
                }
                else
                {
                    if (!string.IsNullOrEmpty(contractAttribute.Name))
                    {
                        recordResolution.Name = new (contractAttribute.Name, true);
                    }

                    if (!string.IsNullOrEmpty(contractAttribute.Namespace))
                    {
                        recordResolution.Namespace = new (contractAttribute.Namespace, true);
                    }

                    recordResolution.Fields = recordResolution.Fields
                        .SelectMany(field =>
                        {
                            var dataMemberAttribute = field.Member.GetAttribute<DataMemberAttribute>();

                            if (dataMemberAttribute == null)
                            {
                                return Array.Empty<FieldResolution>();
                            }

                            if (!string.IsNullOrEmpty(dataMemberAttribute.Name))
                            {
                                field.Name = new (dataMemberAttribute.Name, true);
                            }

                            return new[] { field };
                        })
                        .OrderBy(resolution => resolution.Name.Value)
                        .ToList();
                }
            }

            return result;
        }
    }
}
