namespace Chr.Avro.Infrastructure
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Provides methods that simplify working with <see cref="ConstrainedSet{T}" />.
    /// </summary>
    internal static class ConstrainedSetExtensions
    {
        /// <summary>
        /// Creates a <see cref="ConstrainedSet{T}" /> containing all the elements of the
        /// <see cref="IEnumerable{T}" />.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the elements in the <see cref="IEnumerable{T}" />.
        /// </typeparam>
        /// <param name="enumerable">
        /// An <see cref="IEnumerable{T}" /> whose elements should be copied to the new
        /// <see cref="ConstrainedSet{T}" />.
        /// </param>
        /// <param name="predicate">
        /// A function that will be used to determine whether an element can be added to the
        /// <see cref="ConstrainedSet{T}" />. The first parameter is the element to test and the
        /// second parameter is the set instance. If no predicate function is provided, all
        /// elements will be allowed.
        /// </param>
        /// <returns>
        /// A new <see cref="ConstrainedSet{T}" /> instance.
        /// </returns>
        internal static ConstrainedSet<T> ToConstrainedSet<T>(this IEnumerable<T> enumerable, Func<T, ConstrainedSet<T>, bool>? predicate = null)
        {
            if (enumerable == null)
            {
                throw new ArgumentNullException(nameof(enumerable));
            }

            return new ConstrainedSet<T>(enumerable, predicate);
        }
    }
}
