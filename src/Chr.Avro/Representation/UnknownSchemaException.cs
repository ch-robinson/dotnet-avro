using System;

namespace Chr.Avro.Representation
{
    /// <summary>
    /// The exception that is thrown when a schema representation cannot be parsed.
    /// </summary>
    [Serializable]
    public class UnknownSchemaException : Exception
    {
        /// <summary>
        /// Creates an exception describing the error.
        /// </summary>
        /// <param name="message">
        /// A message describing the exception.
        /// </param>
        /// <param name="inner">
        /// An underlying error that may provide additional context.
        /// </param>
        public UnknownSchemaException(string message, Exception inner = null) : base(message, inner) { }
    }
}
