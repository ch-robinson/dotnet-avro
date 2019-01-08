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
        /// this method should throw an exception.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        TypeResolution Apply(Type type);
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
        private ICollection<ITypeResolverCase> cases;

        /// <summary>
        /// A list of cases that the resolver will attempt to apply. If the first case fails, the
        /// resolver will try the next case, and so on until all cases have been attempted.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the case collection is set to null.
        /// </exception>
        public virtual ICollection<ITypeResolverCase> Cases
        {
            get
            {
                return cases ?? throw new InvalidOperationException();
            }
            set
            {
                cases = value ?? throw new ArgumentNullException(nameof(value), "The resolver case collection cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new type resolver.
        /// </summary>
        /// <param name="cases">
        /// An optional collection of cases. If no case collection is provided, <see cref="Cases" />
        /// will be empty.
        /// </param>
        public TypeResolver(ICollection<ITypeResolverCase> cases = null)
        {
            Cases = cases ?? new List<ITypeResolverCase>();
        }

        /// <summary>
        /// Resolves information for a .NET type, trying each case until a match is found.
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
        /// Resolves information for a .NET type, trying each case until a match is found.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no matching case is found for the type. <see cref="Exception.InnerException" />
        /// will be an <see cref="AggregateException" /> containing the exceptions thrown by each
        /// attempted case.
        /// </exception>
        public virtual TypeResolution ResolveType(Type type)
        {
            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                try
                {
                    return @case.Apply(type);
                }
                catch (Exception exception)
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
        /// this method should throw an exception.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="TypeResolution" /> that contains information about the type.
        /// </returns>
        public abstract TypeResolution Apply(Type type);
    }
}
