namespace Chr.Avro.Resolution
{
    using System;
    using System.Linq;
    using System.Runtime.Serialization;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="Enum" />s, taking
    /// <see cref="System.Runtime.Serialization" /> attributes into account.
    /// </summary>
    public class DataContractEnumTypeResolverCase : EnumTypeResolverCase
    {
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with an <see cref="EnumResolution" />
        /// if <paramref name="type" /> is an <see cref="Enum" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise. If <paramref name="type" /> has <see cref="DataContractAttribute" />, the
        /// <see cref="EnumResolution" /> will only include symbols that have
        /// <see cref="EnumMemberAttribute" />. If <paramref name="type" /> does not have
        /// <see cref="DataContractAttribute" />, the <see cref="EnumResolution" /> will include
        /// all symbols that do not have <see cref="NonSerializedAttribute" />.
        /// </returns>
        /// <inheritdoc />
        public override TypeResolverCaseResult ResolveType(Type type)
        {
            var result = base.ResolveType(type);

            if (result.TypeResolution is EnumResolution enumResolution)
            {
                var contractAttribute = type.GetAttribute<DataContractAttribute>();

                if (contractAttribute == null)
                {
                    enumResolution.Symbols = enumResolution.Symbols
                        .Where(symbol => symbol.Member.GetAttribute<NonSerializedAttribute>() == null)
                        .ToList();
                }
                else
                {
                    if (!string.IsNullOrEmpty(contractAttribute.Name))
                    {
                        enumResolution.Name = new (contractAttribute.Name, true);
                    }

                    if (!string.IsNullOrEmpty(contractAttribute.Namespace))
                    {
                        enumResolution.Namespace = new (contractAttribute.Namespace, true);
                    }

                    enumResolution.Symbols = enumResolution.Symbols
                        .SelectMany(symbol =>
                        {
                            var enumMemberAttribute = symbol.Member.GetAttribute<EnumMemberAttribute>();

                            if (enumMemberAttribute == null)
                            {
                                return Array.Empty<SymbolResolution>();
                            }

                            if (!string.IsNullOrEmpty(enumMemberAttribute.Value))
                            {
                                symbol.Name = new (enumMemberAttribute.Value, true);
                            }

                            return new[] { symbol };
                        })
                        .OrderBy(resolution => resolution.Value)
                        .ThenBy(resolution => resolution.Name.Value)
                        .ToList();
                }
            }

            return result;
        }
    }
}
