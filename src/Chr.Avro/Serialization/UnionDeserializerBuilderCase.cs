namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="UnionSchema" />.
    /// </summary>
    public class UnionDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <summary>
        /// Customizes type resolutions for the children of a <see cref="UnionSchema" />. Can be
        /// overriden by custom cases to support polymorphic mapping.
        /// </summary>
        /// <param name="resolution">
        /// A <see cref="TypeResolution" /> being mapped to a <see cref="UnionSchema" />.
        /// </param>
        /// <param name="schema">
        /// A child of the <see cref="UnionSchema" />.
        /// </param>
        /// <returns>
        /// A <see cref="TypeResolution" /> to build the child deserializer with. The
        /// <see cref="Type" /> in the original <see cref="TypeResolution" /> must be assignable
        /// from the type in the returned <see cref="TypeResolution" />.
        /// </returns>
        protected virtual TypeResolution SelectType(TypeResolution resolution, Schema schema)
        {
            return resolution;
        }
    }
}
