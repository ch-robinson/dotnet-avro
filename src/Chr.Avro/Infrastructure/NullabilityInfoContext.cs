namespace Chr.Avro.Infrastructure
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Provides APIs for populating nullability information/context from reflection members
    /// <see cref="FieldInfo" /> and <see cref="PropertyInfo" />.
    /// </summary>
    /// <remarks>
    /// This type is a stand-in for the <c>NullabilityInfoContext</c> class available in .NET 6 and above. See
    /// <see href="https://github.com/dotnet/runtime/blob/v6.0.0/src/libraries/System.Private.CoreLib/src/System/Reflection/NullabilityInfoContext.cs">the .NET runtime source</see>
    /// (also MIT licensed) for the reference implementation.
    /// </remarks>
    internal sealed class NullabilityInfoContext
    {
        private const string CompilerServicesNamespace = "System.Runtime.CompilerServices";

        private readonly Dictionary<MemberInfo, NullabilityState> context = new();
        private readonly Dictionary<Module, NotAnnotatedStatus> publicOnlyModules = new();

        [Flags]
        private enum NotAnnotatedStatus
        {
            /// <summary>
            /// No restriction, all members annotated.
            /// </summary>
            None = 0x0,

            /// <summary>
            /// Private members not annotated.
            /// </summary>
            Private = 0x1,

            /// <summary>
            /// Internal members not annotated.
            /// </summary>
            Internal = 0x2,
        }

        /// <summary>
        /// Populates a <see cref="NullabilityInfo" /> for the given <see cref="FieldInfo" />. If
        /// the <c>nullablePublicOnly</c> feature is set for an assembly, like it does in the .NET
        /// SDK, the private and/or internal member's nullability attributes are omitted, and the
        /// API will return the <see cref="NullabilityState.Unknown" /> state.
        /// </summary>
        /// <param name="fieldInfo">
        /// The field for which to populate the nullability information.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="fieldInfo" /> is <c>null</c>.
        /// </exception>
        /// <returns>
        /// A <see cref="NullabilityInfo" /> instance.
        /// </returns>
        public NullabilityInfo Create(FieldInfo fieldInfo)
        {
            var attributes = fieldInfo.GetCustomAttributesData();
            var parser = IsPrivateOrInternalFieldAndAnnotationDisabled(fieldInfo)
                ? NullableAttributeStateParser.Unknown
                : CreateParser(attributes);

            var nullability = GetNullabilityInfo(fieldInfo, fieldInfo.FieldType, parser);
            CheckNullabilityAttributes(nullability, attributes);

            return nullability;
        }

        /// <summary>
        /// Populates a <see cref="NullabilityInfo" /> for the given <see cref="PropertyInfo" />.
        /// If the <c>nullablePublicOnly</c> feature is set for an assembly, like it does in the
        /// .NET SDK, the private and/or internal member's nullability attributes are omitted, and
        /// the API will return the <see cref="NullabilityState.Unknown" /> state.
        /// </summary>
        /// <param name="propertyInfo">
        /// The property for which to populate the nullability information.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="propertyInfo" /> is <c>null</c>.
        /// </exception>
        /// <returns>
        /// A <see cref="NullabilityInfo" /> instance.
        /// </returns>
        public NullabilityInfo Create(PropertyInfo propertyInfo)
        {
            var getter = propertyInfo.GetGetMethod(true);
            var setter = propertyInfo.GetSetMethod(true);
            var annotationsDisabled = (getter == null || IsPrivateOrInternalMethodAndAnnotationDisabled(getter))
                && (setter == null || IsPrivateOrInternalMethodAndAnnotationDisabled(setter));
            var parser = annotationsDisabled
                ? NullableAttributeStateParser.Unknown
                : CreateParser(propertyInfo.GetCustomAttributesData());

            var nullability = GetNullabilityInfo(propertyInfo, propertyInfo.PropertyType, parser);

            if (getter != null)
            {
                CheckNullabilityAttributes(nullability, getter.ReturnParameter.GetCustomAttributesData());
            }
            else
            {
                nullability.ReadState = NullabilityState.Unknown;
            }

            if (setter != null)
            {
                CheckNullabilityAttributes(nullability, setter.GetParameters().Last().GetCustomAttributesData());
            }
            else
            {
                nullability.WriteState = NullabilityState.Unknown;
            }

            return nullability;
        }

        private static NullableAttributeStateParser CreateParser(IList<CustomAttributeData> customAttributes)
        {
            foreach (var attribute in customAttributes)
            {
                if (attribute.AttributeType.Name == "NullableAttribute" &&
                    attribute.AttributeType.Namespace == CompilerServicesNamespace &&
                    attribute.ConstructorArguments.Count == 1)
                {
                    return new(attribute.ConstructorArguments[0].Value);
                }
            }

            return new(null);
        }

        private static MemberInfo GetMemberMetadataDefinition(MemberInfo member)
        {
            var type = member.DeclaringType;

            if ((type != null) && type.IsGenericType && !type.IsGenericTypeDefinition)
            {
                return type.GetGenericTypeDefinition().GetMemberWithSameMetadataDefinitionAs(member);
            }

            return member;
        }

        private static Type GetPropertyMetaType(PropertyInfo property)
        {
            if (property.GetGetMethod(true) is MethodInfo method)
            {
                return method.ReturnType;
            }

            return property.GetSetMethod(true)!.GetParameters()[0].ParameterType;
        }

        private static NullabilityState TranslateByte(object? value)
        {
            return value is byte b
                ? TranslateByte(b)
                : NullabilityState.Unknown;
        }

        private static NullabilityState TranslateByte(byte b)
        {
            return b switch
            {
                1 => NullabilityState.NotNull,
                2 => NullabilityState.Nullable,
                _ => NullabilityState.Unknown,
            };
        }

        private void CheckGenericParameters(NullabilityInfo nullability, MemberInfo metaMember, Type metaType, Type? reflectedType)
        {
            if (metaType.IsGenericParameter)
            {
                if (nullability.ReadState == NullabilityState.NotNull)
                {
                    TryUpdateGenericParameterNullability(nullability, metaType, reflectedType);
                }
            }
            else if (metaType.ContainsGenericParameters)
            {
                if (nullability.GenericTypeArguments.Length > 0)
                {
                    var genericArguments = metaType.GetGenericArguments();

                    for (var i = 0; i < genericArguments.Length; i++)
                    {
                        CheckGenericParameters(nullability.GenericTypeArguments[i], metaMember, genericArguments[i], reflectedType);
                    }
                }
                else if (nullability.ElementType is NullabilityInfo elementNullability && metaType.IsArray)
                {
                    CheckGenericParameters(elementNullability, metaMember, metaType.GetElementType()!, reflectedType);
                }
            }
        }

        private void CheckNullabilityAttributes(NullabilityInfo nullability, IList<CustomAttributeData> attributes)
        {
            var codeAnalysisReadState = NullabilityState.Unknown;
            var codeAnalysisWriteState = NullabilityState.Unknown;

            foreach (CustomAttributeData attribute in attributes)
            {
                if (attribute.AttributeType.Namespace == "System.Diagnostics.CodeAnalysis")
                {
                    if (attribute.AttributeType.Name == "NotNullAttribute")
                    {
                        codeAnalysisReadState = NullabilityState.NotNull;
                    }
                    else if ((attribute.AttributeType.Name == "MaybeNullAttribute" ||
                        attribute.AttributeType.Name == "MaybeNullWhenAttribute") &&
                        codeAnalysisReadState == NullabilityState.Unknown &&
                        !nullability.Type.IsValueType)
                    {
                        codeAnalysisReadState = NullabilityState.Nullable;
                    }
                    else if (attribute.AttributeType.Name == "DisallowNullAttribute")
                    {
                        codeAnalysisWriteState = NullabilityState.NotNull;
                    }
                    else if (attribute.AttributeType.Name == "AllowNullAttribute" &&
                        codeAnalysisWriteState == NullabilityState.Unknown &&
                        !nullability.Type.IsValueType)
                    {
                        codeAnalysisWriteState = NullabilityState.Nullable;
                    }
                }
            }

            if (codeAnalysisReadState != NullabilityState.Unknown)
            {
                nullability.ReadState = codeAnalysisReadState;
            }

            if (codeAnalysisWriteState != NullabilityState.Unknown)
            {
                nullability.WriteState = codeAnalysisWriteState;
            }
        }

        private NullabilityState? GetNullableContext(MemberInfo? memberInfo)
        {
            while (memberInfo != null)
            {
                if (context.TryGetValue(memberInfo, out NullabilityState state))
                {
                    return state;
                }

                foreach (CustomAttributeData attribute in memberInfo.GetCustomAttributesData())
                {
                    if (attribute.AttributeType.Name == "NullableContextAttribute" &&
                        attribute.AttributeType.Namespace == CompilerServicesNamespace &&
                        attribute.ConstructorArguments.Count == 1)
                    {
                        state = TranslateByte(attribute.ConstructorArguments[0].Value);
                        context.Add(memberInfo, state);

                        return state;
                    }
                }

                memberInfo = memberInfo.DeclaringType;
            }

            return null;
        }

        private NullabilityInfo GetNullabilityInfo(MemberInfo memberInfo, Type type, NullableAttributeStateParser parser)
        {
            var index = 0;

            return GetNullabilityInfo(memberInfo, type, parser, ref index);
        }

        private NullabilityInfo GetNullabilityInfo(MemberInfo memberInfo, Type type, NullableAttributeStateParser parser, ref int index)
        {
            var state = NullabilityState.Unknown;
            var elementState = (NullabilityInfo?)null;
            var genericArgumentsState = Array.Empty<NullabilityInfo>();
            var underlyingType = type;

            if (type.IsValueType)
            {
                underlyingType = Nullable.GetUnderlyingType(type);

                if (underlyingType != null)
                {
                    state = NullabilityState.Nullable;
                }
                else
                {
                    underlyingType = type;
                    state = NullabilityState.NotNull;
                }

                if (underlyingType.IsGenericType)
                {
                    ++index;
                }
            }
            else
            {
                if (!parser.ParseNullableState(index++, ref state)
                    && GetNullableContext(memberInfo) is NullabilityState contextState)
                {
                    state = contextState;
                }

                if (type.IsArray)
                {
                    elementState = GetNullabilityInfo(memberInfo, type.GetElementType()!, parser, ref index);
                }
            }

            if (underlyingType.IsGenericType)
            {
                var genericArguments = underlyingType.GetGenericArguments();
                genericArgumentsState = new NullabilityInfo[genericArguments.Length];

                for (int i = 0; i < genericArguments.Length; i++)
                {
                    genericArgumentsState[i] = GetNullabilityInfo(memberInfo, genericArguments[i], parser, ref index);
                }
            }

            var nullability = new NullabilityInfo(type, state, state, elementState, genericArgumentsState);

            if (!type.IsValueType && state != NullabilityState.Unknown)
            {
                TryLoadGenericMetaTypeNullability(memberInfo, nullability);
            }

            return nullability;
        }

        private bool IsPrivateOrInternalFieldAndAnnotationDisabled(FieldInfo fieldInfo)
        {
            return (fieldInfo.IsPrivate || fieldInfo.IsFamilyAndAssembly || fieldInfo.IsAssembly) &&
                IsPublicOnly(fieldInfo.IsPrivate, fieldInfo.IsFamilyAndAssembly, fieldInfo.IsAssembly, fieldInfo.Module);
        }

        private bool IsPrivateOrInternalMethodAndAnnotationDisabled(MethodBase method)
        {
            return (method.IsPrivate || method.IsFamilyAndAssembly || method.IsAssembly) &&
               IsPublicOnly(method.IsPrivate, method.IsFamilyAndAssembly, method.IsAssembly, method.Module);
        }

        private bool IsPublicOnly(bool isPrivate, bool isFamilyAndAssembly, bool isAssembly, Module module)
        {
            if (!publicOnlyModules.TryGetValue(module, out NotAnnotatedStatus value))
            {
                value = PopulateAnnotationInfo(module.GetCustomAttributesData());
                publicOnlyModules.Add(module, value);
            }

            if (value == NotAnnotatedStatus.None)
            {
                return false;
            }

            return ((isPrivate || isFamilyAndAssembly) && value.HasFlag(NotAnnotatedStatus.Private)) ||
                (isAssembly && value.HasFlag(NotAnnotatedStatus.Internal));
        }

        private NotAnnotatedStatus PopulateAnnotationInfo(IList<CustomAttributeData> customAttributes)
        {
            foreach (CustomAttributeData attribute in customAttributes)
            {
                if (attribute.AttributeType.Name == "NullablePublicOnlyAttribute" &&
                    attribute.AttributeType.Namespace == CompilerServicesNamespace &&
                    attribute.ConstructorArguments.Count == 1)
                {
                    if (attribute.ConstructorArguments[0].Value is bool boolValue && boolValue)
                    {
                        return NotAnnotatedStatus.Internal | NotAnnotatedStatus.Private;
                    }
                    else
                    {
                        return NotAnnotatedStatus.Private;
                    }
                }
            }

            return NotAnnotatedStatus.None;
        }

        private void TryLoadGenericMetaTypeNullability(MemberInfo memberInfo, NullabilityInfo nullability)
        {
            var metaMember = GetMemberMetadataDefinition(memberInfo);
            var metaType = (Type?)null;

            if (metaMember is FieldInfo field)
            {
                metaType = field.FieldType;
            }
            else if (metaMember is PropertyInfo property)
            {
                metaType = GetPropertyMetaType(property);
            }

            if (metaType != null)
            {
                CheckGenericParameters(nullability, metaMember!, metaType, memberInfo.ReflectedType);
            }
        }

        private bool TryUpdateGenericParameterNullability(NullabilityInfo nullability, Type genericParameter, Type? reflectedType)
        {
            if (reflectedType is not null
                && !genericParameter.IsGenericMethodParameter()
                && TryUpdateGenericTypeParameterNullabilityFromReflectedType(nullability, genericParameter, reflectedType, reflectedType))
            {
                return true;
            }

            var state = NullabilityState.Unknown;

            if (CreateParser(genericParameter.GetCustomAttributesData()).ParseNullableState(0, ref state))
            {
                nullability.ReadState = state;
                nullability.WriteState = state;
                return true;
            }

            if (GetNullableContext(genericParameter) is { } contextState)
            {
                nullability.ReadState = contextState;
                nullability.WriteState = contextState;
                return true;
            }

            return false;
        }

        private bool TryUpdateGenericTypeParameterNullabilityFromReflectedType(NullabilityInfo nullability, Type genericParameter, Type context, Type reflectedType)
        {
            var contextTypeDefinition = context.IsGenericType && !context.IsGenericTypeDefinition
                ? context.GetGenericTypeDefinition()
                : context;

            if (genericParameter.DeclaringType == contextTypeDefinition)
            {
                return false;
            }

            var baseType = contextTypeDefinition.BaseType;

            if (baseType is null)
            {
                return false;
            }

            if (!baseType.IsGenericType
                || (baseType.IsGenericTypeDefinition ? baseType : baseType.GetGenericTypeDefinition()) != genericParameter.DeclaringType)
            {
                return TryUpdateGenericTypeParameterNullabilityFromReflectedType(nullability, genericParameter, baseType, reflectedType);
            }

            var genericArguments = baseType.GetGenericArguments();
            var genericArgument = genericArguments[genericParameter.GenericParameterPosition];

            if (genericArgument.IsGenericParameter)
            {
                return TryUpdateGenericParameterNullability(nullability, genericArgument, reflectedType);
            }

            var parser = CreateParser(contextTypeDefinition.GetCustomAttributesData());
            var nullabilityStateIndex = 1; // start at 1 since index 0 is the type itself

            for (int i = 0; i < genericParameter.GenericParameterPosition; i++)
            {
                nullabilityStateIndex += CountNullabilityStates(genericArguments[i]);
            }

            return TryPopulateNullabilityInfo(nullability, parser, ref nullabilityStateIndex);

            static int CountNullabilityStates(Type type)
            {
                var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

                if (underlyingType.IsGenericType)
                {
                    var count = 1;

                    foreach (Type genericArgument in underlyingType.GetGenericArguments())
                    {
                        count += CountNullabilityStates(genericArgument);
                    }

                    return count;
                }

                if (underlyingType.IsArray)
                {
                    return 1 + CountNullabilityStates(underlyingType.GetElementType()!);
                }

                return type.IsValueType ? 0 : 1;
            }
        }

        private bool TryPopulateNullabilityInfo(NullabilityInfo nullability, NullableAttributeStateParser parser, ref int index)
        {
            var isValueType = nullability.Type.IsValueType;

            if (!isValueType)
            {
                var state = NullabilityState.Unknown;

                if (!parser.ParseNullableState(index, ref state))
                {
                    return false;
                }

                nullability.ReadState = state;
                nullability.WriteState = state;
            }

            if (!isValueType || (Nullable.GetUnderlyingType(nullability.Type) ?? nullability.Type).IsGenericType)
            {
                index++;
            }

            if (nullability.GenericTypeArguments.Length > 0)
            {
                foreach (NullabilityInfo genericTypeArgumentNullability in nullability.GenericTypeArguments)
                {
                    TryPopulateNullabilityInfo(genericTypeArgumentNullability, parser, ref index);
                }
            }
            else if (nullability.ElementType is { } elementTypeNullability)
            {
                TryPopulateNullabilityInfo(elementTypeNullability, parser, ref index);
            }

            return true;
        }

        private readonly struct NullableAttributeStateParser
        {
            private static readonly object UnknownByte = (byte)0;

            private readonly object? nullableAttributeArgument;

            public NullableAttributeStateParser(object? nullableAttributeArgument)
            {
                this.nullableAttributeArgument = nullableAttributeArgument;
            }

            public static NullableAttributeStateParser Unknown => new(UnknownByte);

            public bool ParseNullableState(int index, ref NullabilityState state)
            {
                switch (nullableAttributeArgument)
                {
                    case byte b:
                        state = TranslateByte(b);
                        return true;
                    case ReadOnlyCollection<CustomAttributeTypedArgument> args
                        when index < args.Count && args[index].Value is byte elementB:
                        state = TranslateByte(elementB);
                        return true;
                    default:
                        return false;
                }
            }
        }
    }
}
