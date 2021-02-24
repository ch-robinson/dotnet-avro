namespace Chr.Avro.Resolution
{
    using System;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that inspects fields and properties.
    /// </summary>
    public class ObjectTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ObjectTypeResolverCase" /> class.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        public ObjectTypeResolverCase(BindingFlags memberVisibility)
        {
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Gets the binding flags used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Resolves class, interface, or struct <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="RecordResolution" />
        /// if <paramref name="type" /> is not an array or primitive type; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (!type.IsArray && !type.IsPrimitive)
            {
                var name = new IdentifierResolution(type.Name);

                var @namespace = string.IsNullOrEmpty(type.Namespace)
                    ? null
                    : new IdentifierResolution(type.Namespace);

                var fields = type.GetMembers(MemberVisibility)
                    .SelectMany(member => member switch
                    {
                        FieldInfo field => new[] { new FieldResolution(field, field.FieldType, new (field.Name)) },
                        PropertyInfo property => new[] { new FieldResolution(property, property.PropertyType, new (property.Name)) },
                        _ => Array.Empty<FieldResolution>()
                    })
                    .OrderBy(resolution => resolution.Name.Value);

                var constructors = type.GetConstructors(MemberVisibility)
                    .Select(constructor => new ConstructorResolution(
                        constructor,
                        constructor.GetParameters()
                            .Select(parameter => new ParameterResolution(
                                parameter,
                                new IdentifierResolution(parameter.Name)))));

                return TypeResolverCaseResult.FromTypeResolution(new RecordResolution(type, name, @namespace, fields, constructors));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(ObjectTypeResolverCase)} can only be applied to non-array, non-primitive types."));
            }
        }
    }
}
