namespace Chr.Avro.Representation
{
    using System;

    /// <summary>
    /// The exception thrown when a schema representation cannot be parsed.
    /// </summary>
    [Serializable]
    public class UnknownSchemaException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UnknownSchemaException" /> class.
        /// </summary>
        /// <param name="message">
        /// A message describing the exception.
        /// </param>
        /// <param name="inner">
        /// An underlying exception that may provide additional context.
        /// </param>
        public UnknownSchemaException(string message, Exception? inner = null)
            : base(message, inner)
        {
        }
    }
}
