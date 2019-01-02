using System;

namespace Chr.Avro.Abstract
{
    /// <summary>
    /// The exception that is thrown when a schema contraint is violated.
    /// </summary>
    /// <remarks>
    /// This exception should only be thrown for abstract constraint violations.
    /// </remarks>
    [Serializable]
    public class InvalidSchemaException : Exception
    {
        /// <summary>
        /// Creates an exception describing the invalid schema.
        /// </summary>
        /// <param name="message">
        /// A description of the constraint violation.
        /// </param>
        public InvalidSchemaException(string message) : base(message) { }
    }
}
