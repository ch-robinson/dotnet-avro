using System;
using System.Collections;
using System.Collections.Generic;

namespace Chr.Avro.Infrastructure
{
    /// <summary>
    /// An ordered set implementation with an optional predicate.
    /// </summary>
    /// <remarks>
    /// Adapted from a <a href="https://stackoverflow.com/a/17853085">Stack Overflow answer</a>.
    /// </remarks>
    internal class ConstrainedSet<T> : ICollection<T>
    {
        private readonly IDictionary<T, LinkedListNode<T>> _dictionary;

        private readonly LinkedList<T> _list;

        protected readonly Func<T, ConstrainedSet<T>, bool> Predicate;

        public int Count => _dictionary.Count;

        public bool IsReadOnly => _dictionary.IsReadOnly;

        public ConstrainedSet(IEnumerable<T>? existing = null, Func<T, ConstrainedSet<T>, bool>? predicate = null)
        {
            _dictionary = new Dictionary<T, LinkedListNode<T>>();
            _list = new LinkedList<T>();

            Predicate = predicate ?? ((s, c) => true);

            if (existing != null)
            {
                foreach (var item in existing)
                {
                    Add(item);
                }
            }
        }

        public virtual void Add(T item)
        {
            if (_dictionary.ContainsKey(item) || !Predicate(item, this))
            {
                return;
            }

            var node = _list.AddLast(item);
            _dictionary.Add(item, node);
        }

        public virtual void Clear()
        {
            _dictionary.Clear();
            _list.Clear();
        }

        public virtual bool Contains(T item)
        {
            return _dictionary.ContainsKey(item);
        }

        public void CopyTo(T[] array, int index)
        {
            _list.CopyTo(array, index);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        public virtual bool Remove(T item)
        {
            if (!_dictionary.TryGetValue(item, out var node))
            {
                return false;
            }

            _dictionary.Remove(item);
            _list.Remove(node);

            return true;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    internal static class ConstrainedSetExtensions
    {
        internal static ConstrainedSet<T> ToConstrainedSet<T>(this IEnumerable<T> enumerable, Func<T, ConstrainedSet<T>, bool>? predicate = null)
        {
            return new ConstrainedSet<T>(enumerable, predicate);
        }
    }
}
