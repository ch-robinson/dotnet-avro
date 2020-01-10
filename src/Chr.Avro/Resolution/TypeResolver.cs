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
        TypeResolution ResolveType(Type type);
    }

    /// <summary>
    /// Represents the outcome of a type resolver case.
    /// </summary>
    public interface ITypeResolutionResult
    {
        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="TypeResolution" />
        /// is not null, these exceptions should be interpreted as warnings.
        /// </summary>
        ICollection<Exception> Exceptions { get; }

        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        TypeResolution? TypeResolution { get; }
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
        ITypeResolutionResult ResolveType(Type type);
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
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public TypeResolver(bool resolveReferenceTypesAsNullable = false) : this(new List<Func<TypeResolver, ITypeResolverCase>>(), resolveReferenceTypesAsNullable) { }

        /// <summary>
        /// Creates a new type resolver.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public TypeResolver(IEnumerable<Func<TypeResolver, ITypeResolverCase>> caseBuilders, bool resolveReferenceTypesAsNullable = false)
        {
            var cases = new List<ITypeResolverCase>();

            Cases = cases;
            ResolveReferenceTypesAsNullable = resolveReferenceTypesAsNullable;

            // initialize cases last so that the type resolver is fully ready:
            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
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
        /// <exception cref="AggregateException">
        /// Thrown when no case matches the type. <see cref="AggregateException.InnerExceptions" />
        /// will be contain the exceptions thrown by each case.
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
        /// Thrown when no case matches the type.
        /// </exception>
        public virtual TypeResolution ResolveType(Type type)
        {
            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                var result = @case.ResolveType(type);

                if (result.TypeResolution != null)
                {
                    if (ResolveReferenceTypesAsNullable && !type.IsValueType)
                    {
                        result.TypeResolution.IsNullable = true;
                    }

                    return result.TypeResolution;
                }

                exceptions.AddRange(result.Exceptions);
            }

            throw new UnsupportedTypeException(type, $"No type resolver case could be applied to {type.FullName}.", new AggregateException(exceptions));
        }
    }

    /// <summary>
    /// A base <see cref="ITypeResolutionResult" /> implementation.
    /// </summary>
    public class TypeResolutionResult : ITypeResolutionResult
    {
        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="TypeResolution" />
        /// is not null, these exceptions should be interpreted as warnings.
        /// </summary>
        public ICollection<Exception> Exceptions { get; set; } = new List<Exception>();

        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        public TypeResolution? TypeResolution { get; set; }
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
        /// Resolves information for a .NET type.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A resolution result.
        /// </returns>
        public abstract ITypeResolutionResult ResolveType(Type type);
    }
}
