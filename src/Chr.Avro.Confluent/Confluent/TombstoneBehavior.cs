namespace Chr.Avro.Confluent
{
    /// <summary>
    /// Options for serializing and deserializing null record components.
    /// </summary>
    public enum TombstoneBehavior
    {
        /// <summary>
        /// Do not support tombstones. Require that record keys and values conform to the Confluent
        /// wire format.
        /// </summary>
        None,

        /// <summary>
        /// Support tombstones with strict correctness requirements. Require that record keys
        /// conform to the Confluent wire format and that any type mapped to a record value is
        /// either a reference type or nullable value type.
        /// </summary>
        Strict,
    }
}
