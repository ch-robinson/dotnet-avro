namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="ArraySchema" />.
    /// </summary>
    public abstract class ArraySerializerBuilderCase : SerializerBuilderCase
    {
        /// <summary>
        /// Gets the item <see cref="Type" /> of an enumerable <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> object that describes a generic enumerable.
        /// </param>
        /// <returns>
        /// If <paramref name="type" /> implements (or is) <see cref="IEnumerable{T}" />, its type
        /// argument; <c>null</c> otherwise.
        /// </returns>
        protected virtual Type? GetEnumerableType(Type type)
        {
            return type.GetEnumerableType();
        }
    }
}
