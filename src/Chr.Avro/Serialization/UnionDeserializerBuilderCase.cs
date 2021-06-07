namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="UnionSchema" />.
    /// </summary>
    public class UnionDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <summary>
        /// Customizes type selection for the children of a <see cref="UnionSchema" />. Can be
        /// overriden by custom cases to support polymorphic mapping.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> being mapped to a <see cref="UnionSchema" />.
        /// </param>
        /// <param name="schema">
        /// A child of the <see cref="UnionSchema" />.
        /// </param>
        /// <returns>
        /// A <see cref="Type" /> to build the child deserializer with. <paramref name="type" />
        /// must be assignable from the returned <see cref="Type" />.
        /// </returns>
        protected virtual Type SelectType(Type type, Schema schema)
        {
            return type;
        }
    }
}
