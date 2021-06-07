namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="MapSchema" />.
    /// </summary>
    public abstract class MapSerializerBuilderCase : SerializerBuilderCase
    {
        /// <summary>
        /// Gets the item <see cref="Type" /> of a dictionary <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> object that describes a generic dictionary.
        /// </param>
        /// <returns>
        /// If <paramref name="type" /> implements (or is) <see cref="IEnumerable{T}" /> and the
        /// item type is <see cref="KeyValuePair{TKey, TValue}" />, its type arguments; <c>null</c>
        /// otherwise.
        /// </returns>
        protected virtual (Type Key, Type Value)? GetDictionaryTypes(Type type)
        {
            return type.GetDictionaryTypes();
        }
    }
}
