namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Represents an Avro schema.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#schemas">Avro spec</a> for
    /// details.
    /// </remarks>
    public abstract class Schema : Declaration
    {
        /// <summary>
        /// Gets or sets the schema’s logical type.
        /// </summary>
        public LogicalType? LogicalType { get; set; }

        /// <inheritdoc />
        public override string ToString()
        {
            return GetType().Name;
        }
    }
}
