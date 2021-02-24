namespace Chr.Avro.Resolution
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to resolve information for .NET <see cref="Type" />.
    /// </summary>
    /// <remarks>
    /// The type resolution framework is a light abstraction around the .NET type system. It allows
    /// most Chr.Avro behaviors to be customized and keeps reflection logic out of components like
    /// the <see cref="SchemaBuilder" />.
    /// </remarks>
    public interface ITypeResolver
    {
        /// <summary>
        /// Resolves information for a .NET <see cref="Type" />.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> to resolve.
        /// </typeparam>
        /// <returns>
        /// A <see cref="TypeResolution" /> representing information about <typeparamref name="T" />.
        /// </returns>
        TypeResolution ResolveType<T>();

        /// <summary>
        /// Resolves information for a .NET <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// The <see cref="Type" /> to resolve.
        /// </param>
        /// <returns>
        /// A <see cref="TypeResolution" /> representing information about <paramref name="type" />.
        /// </returns>
        TypeResolution ResolveType(Type type);
    }
}
