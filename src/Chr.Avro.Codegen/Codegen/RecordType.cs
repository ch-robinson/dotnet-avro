namespace Chr.Avro.Codegen
{
    /// <summary>
    /// Options for representing Avro records in C#.
    /// </summary>
    public enum RecordType
    {
        /// <summary>
        /// Represent Avro records as C# classes.
        /// </summary>
        Class,

        /// <summary>
        /// Represent Avro records as C# records.
        /// </summary>
        Record,
    }
}
