namespace Chr.Avro.Infrastructure
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Implements an ordered set with an optional predicate.
    /// </summary>
    /// <remarks>
    /// This collection type reduces the amount of code needed to implement some aspects of the
    /// abstract schema model (for instance, the requirements that named schemas cannot have
    /// identical aliases and that enum schemas cannot have duplicate symbols). The implementation
    /// was adapted from a <a href="https://stackoverflow.com/a/17853085">Stack Overflow answer</a>.
    /// </remarks>
    /// <typeparam name="T">
    /// The type of the elements in the <see cref="ConstrainedSet{T}" />.
    /// </typeparam>
    internal class ConstrainedSet<T> : ICollection<T>, IReadOnlyCollection<T>
    {
        private readonly IDictionary<T, LinkedListNode<T>> dictionary;

        private readonly LinkedList<T> list;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConstrainedSet{T}" /> class.
        /// </summary>
        /// <param name="existing">
        /// An <see cref="IEnumerable{T}" /> whose elements should be copied to the new
        /// <see cref="ConstrainedSet{T}" />. If no existing <see cref="IEnumerable{T}" /> is
        /// provided, the <see cref="ConstrainedSet{T}" /> will be empty after initialization.
        /// </param>
        /// <param name="predicate">
        /// A function that will be used to determine whether an element can be added to the
        /// <see cref="ConstrainedSet{T}" />. The first parameter is the element to test and the
        /// second parameter is the set instance. If the predicate function returns <c>false</c>,
        /// <see cref="ConstrainedSet{T}.Add(T)" /> calls will succeed without modifying the
        /// <see cref="ConstrainedSet{T}" />; this indicates that an existing item is equivalent.
        /// However, the predicate may also throw to indicate a constraint violation. If no
        /// predicate function is provided, all elements will be allowed.
        /// </param>
        public ConstrainedSet(IEnumerable<T>? existing = null, Func<T, ConstrainedSet<T>, bool>? predicate = null)
        {
            dictionary = new Dictionary<T, LinkedListNode<T>>();
            list = new LinkedList<T>();

            Predicate = predicate ?? ((s, c) => true);

            if (existing != null)
            {
                foreach (var item in existing)
                {
                    Add(item);
                }
            }
        }

        /// <summary>
        /// Gets the number of elements in the <see cref="ConstrainedSet{T}" />.
        /// </summary>
        public int Count => dictionary.Count;

        /// <summary>
        /// Gets a value indicating whether the <see cref="ConstrainedSet{T}" /> is read only. This
        /// value will always be false.
        /// </summary>
        public bool IsReadOnly => dictionary.IsReadOnly;

        /// <summary>
        /// Gets the function that determines whether an item can be added to the
        /// <see cref="ConstrainedSet{T}" />.
        /// </summary>
        protected Func<T, ConstrainedSet<T>, bool> Predicate { get; }

        /// <summary>
        /// Adds an element to the <see cref="ConstrainedSet{T}" />. If the element is already
        /// present or the predicate rejects it, this method will have no effect.
        /// </summary>
        /// <param name="item">
        /// The object to add to the <see cref="ConstrainedSet{T}" />.
        /// </param>
        public virtual void Add(T item)
        {
            if (dictionary.ContainsKey(item) || !Predicate(item, this))
            {
                return;
            }

            var node = list.AddLast(item);
            dictionary.Add(item, node);
        }

        /// <summary>
        /// Removes all elements from the <see cref="ConstrainedSet{T}" />.
        /// </summary>
        public virtual void Clear()
        {
            dictionary.Clear();
            list.Clear();
        }

        /// <summary>
        /// Determines whether the <see cref="ConstrainedSet{T}" /> contains a specific value.
        /// </summary>
        /// <param name="item">
        /// The object to locate in the <see cref="ConstrainedSet{T}" />.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="item" /> is found in the<see cref="ConstrainedSet{T}" />;
        /// <c>false</c> otherwise.
        /// </returns>
        public virtual bool Contains(T item)
        {
            return dictionary.ContainsKey(item);
        }

        /// <summary>
        /// Copies the elements of the <see cref="ConstrainedSet{T}" /> to an <see cref="Array" />,
        /// starting at a particular <see cref="Array" /> index.
        /// </summary>
        /// <param name="array">
        /// The one-dimensional <see cref="Array" /> that is the destination of the elements copied
        /// from the <see cref="ConstrainedSet{T}" />. The <see cref="Array" /> must have zero-based
        /// indexing.
        /// </param>
        /// <param name="arrayIndex">
        /// The zero-based index in <paramref name="array" /> at which copying begins.
        /// </param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            list.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the <see cref="ConstrainedSet{T}" />.
        /// </summary>
        /// <remarks>
        /// An enumerator remains valid as long as the <see cref="ConstrainedSet{T}" /> remains
        /// unchanged. If changes are made to the <see cref="ConstrainedSet{T}" />, such as adding,
        /// modifying, or removing elements, the enumerator is irrecoverably invalidated and the
        /// next call to <see cref="IEnumerator.MoveNext" /> or <see cref="IEnumerator{T}.Current" />
        /// throws an <see cref="InvalidOperationException" />.
        /// </remarks>
        /// <returns>
        /// An enumerator that iterates through the <see cref="ConstrainedSet{T}" /> in order.
        /// </returns>
        public IEnumerator<T> GetEnumerator()
        {
            return list.GetEnumerator();
        }

        /// <summary>
        /// Removes a specific object from the <see cref="ConstrainedSet{T}" />.
        /// </summary>
        /// <param name="item">
        /// The object to remove from the <see cref="ConstrainedSet{T}" />.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="item" /> was found in the <see cref="ConstrainedSet{T}" />;
        /// <c>false</c> otherwise.
        /// </returns>
        public virtual bool Remove(T item)
        {
            if (!dictionary.TryGetValue(item, out var node))
            {
                return false;
            }

            dictionary.Remove(item);
            list.Remove(node);

            return true;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the <see cref="ConstrainedSet{T}" />.
        /// </summary>
        /// <remarks>
        /// This member is an explicit interface member implementation. It can be used only when
        /// the <see cref="ConstrainedSet{T}" /> is cast to an <see cref="IEnumerable" />
        /// interface.
        /// </remarks>
        /// <returns>
        /// An enumerator that iterates through the <see cref="ConstrainedSet{T}" /> in order.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
