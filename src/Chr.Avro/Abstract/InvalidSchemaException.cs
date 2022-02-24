namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// The exception thrown when a schema contraint is violated.
    /// </summary>
    /// <remarks>
    /// This exception should only be thrown for abstract constraint violations.
    /// </remarks>
    [Serializable]
    public class InvalidSchemaException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidSchemaException" /> class.
        /// </summary>
        /// <param name="message">
        /// A description of the constraint violation.
        /// </param>
        public InvalidSchemaException(string message)
            : base(message)
        {
        }
    }
}
