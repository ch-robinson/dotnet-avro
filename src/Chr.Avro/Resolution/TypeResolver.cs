using System;
using System.Collections.Generic;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// Resolves .NET type information.
    /// </summary>
    /// <remarks>
    /// See the <see cref="TypeResolution" /> class for more information about the resolution
    /// framework.
    /// </remarks>
    public interface ITypeResolver
    {
        /// <summary>
        /// Resolves information for a .NET type.
        /// </summary>
        /// <typeparam name="T">
        /// The type to resolve.
        /// </typeparam>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolver does not support the type.
        /// </exception>
        TypeResolution ResolveType<T>();

        /// <summary>
        /// Resolves information for a .NET type.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolver does not support the type.
        /// </exception>
        TypeResolution ResolveType(Type type);
    }

    /// <summary>
    /// Resolves information for specific .NET types. Used by <see cref="TypeResolver" /> to break
    /// apart resolution logic.
    /// </summary>
    /// <remarks>
    /// See the <see cref="TypeResolution" /> class for more information about the resolution
    /// framework.
    /// </remarks>
    public interface ITypeResolverCase
    {
        /// <summary>
        /// Resolves information for a .NET type. If the case does not apply to the provided type,
        /// this method should throw <see cref="UnsupportedTypeException" />.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        TypeResolution Resolve(Type type);
    }

    /// <summary>
    /// A customizable type resolver backed by a list of cases.
    /// </summary>
    /// <remarks>
    /// See the <see cref="TypeResolution" /> class for more information about the resolution
    /// framework.
    /// </remarks>
    public class TypeResolver : ITypeResolver
    {
        /// <summary>
        /// A list of cases that the resolver will attempt to apply. If the first case does not
        /// match, the resolver will try the next case, and so on until all cases have been tested.
        /// </summary>
        public IEnumerable<ITypeResolverCase> Cases { get; }

        /// <summary>
        /// Whether to resolve reference types as nullable.
        /// </summary>
        public bool ResolveReferenceTypesAsNullable { get; }

        /// <summary>
        /// Creates a new type resolver.
        /// </summary>
        /// <param name="caseBuilders">
        /// An optional list of case builders. If provided, this collection will replace the
        /// default list.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public TypeResolver(IEnumerable<Func<TypeResolver, ITypeResolverCase>> caseBuilders = null, bool resolveReferenceTypesAsNullable = false)
        {
            var cases = new List<ITypeResolverCase>();

            Cases = cases;
            ResolveReferenceTypesAsNullable = resolveReferenceTypesAsNullable;

            // initialize cases last so that the type resolver is fully ready:
            if (caseBuilders != null)
            {
                foreach (var builder in caseBuilders)
                {
                    cases.Add(builder(this));
                }
            }
        }

        /// <summary>
        /// Resolves information for a .NET type.
        /// </summary>
        /// <typeparam name="T">
        /// The type to resolve.
        /// </typeparam>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no matching case is found for the type. <see cref="Exception.InnerException" />
        /// will be an <see cref="AggregateException" /> containing the exceptions thrown by each
        /// attempted case.
        /// </exception>
        public virtual TypeResolution ResolveType<T>()
        {
            return ResolveType(typeof(T));
        }

        /// <summary>
        /// Resolves information for a .NET type.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no matching case is found for the type.
        /// </exception>
        public virtual TypeResolution ResolveType(Type type)
        {
            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                try
                {
                    var resolution = @case.Resolve(type);

                    if (ResolveReferenceTypesAsNullable && !type.IsValueType)
                    {
                        resolution.IsNullable = true;
                    }

                    return resolution;
                }
                catch (UnsupportedTypeException exception)
                {
                    exceptions.Add(exception);
                }
            }

            throw new UnsupportedTypeException(type, $"No type resolver case could be applied to {type.FullName}.", new AggregateException(exceptions));
        }
    }

    /// <summary>
    /// A base <see cref="ITypeResolverCase" /> implementation.
    /// </summary>
    /// <remarks>
    /// See the <see cref="TypeResolution" /> class for more information about the resolution
    /// framework.
    /// </remarks>
    public abstract class TypeResolverCase : ITypeResolverCase
    {
        /// <summary>
        /// Resolves information for a .NET type. If the case does not apply to the provided type,
        /// this method should throw <see cref="UnsupportedTypeException" />.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        public abstract TypeResolution Resolve(Type type);
    }
}
