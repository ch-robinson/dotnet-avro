namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Options for determining nullability of reference types.
    /// </summary>
    public enum NullableReferenceTypeBehavior
    {
        /// <summary>
        /// Assume reference types are never nullable.
        /// </summary>
        None,

        /// <summary>
        /// Match .NET’s nullable semantics, assuming reference types are always nullable.
        /// </summary>
        All,

        /// <summary>
        /// Inspect nullable reference type metadata to infer nullability. For types where metadata
        /// is not present, behavior will be identical to <see cref="None" />.
        /// </summary>
        Annotated,
    }
}
