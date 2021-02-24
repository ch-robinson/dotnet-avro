namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Defines methods to resolve information for specific .NET <see cref="Type" />s.
    /// </summary>
    /// <typeparam name="TResult">
    /// The type of object used to represent the case result.
    /// </typeparam>
    public interface ITypeResolverCase<TResult>
    {
        /// <summary>
        /// Resolves information for a .NET <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// The <see cref="Type" /> to resolve.
        /// </param>
        /// <returns>
        /// A successful <typeparamref name="TResult" /> if the case can be applied;
        /// an unsuccessful <typeparamref name="TResult" /> otherwise.
        /// </returns>
        TResult ResolveType(Type type);
    }
}
