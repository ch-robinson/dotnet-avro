namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Options for building schemas for enum types.
    /// </summary>
    public enum EnumBehavior
    {
        /// <summary>
        /// Build an <see cref="EnumSchema" /> with a symbol for each enum type member.
        /// </summary>
        Symbolic,

        /// <summary>
        /// Build an <see cref="IntSchema" /> or <see cref="LongSchema" /> based on the enum type’s
        /// underlying integral type. This behavior will be used for flag enums regardless of the
        /// behavior selected.
        /// </summary>
        Integral,

        /// <summary>
        /// Build a <see cref="StringSchema" />.
        /// </summary>
        Nominal,
    }
}
