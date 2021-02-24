namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Represents an Avro logical type that can augment primitive or complex types.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Logical+Types">Avro spec</a>
    /// for details.
    /// </remarks>
    public abstract class LogicalType
    {
        /// <inheritdoc />
        public override string ToString()
        {
            return GetType().Name;
        }
    }
}
