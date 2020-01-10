using Chr.Avro.Abstract;
using System;

namespace Chr.Avro
{
    /// <summary>
    /// An exception thrown when an operation does not support a schema.
    /// </summary>
    [Serializable]
    public class UnsupportedSchemaException : Exception
    {
        /// <summary>
        /// The schema that caused the exception to be thrown.
        /// </summary>
        public Schema UnsupportedSchema { get; }

        /// <summary>
        /// Creates an exception describing the error.
        /// </summary>
        /// <param name="schema">
        /// The schema that caused the exception to be thrown.
        /// </param>
        /// <param name="message">
        /// A message describing the exception.
        /// </param>
        /// <param name="inner">
        /// An underlying error that may provide additional context.
        /// </param>
        public UnsupportedSchemaException(Schema schema, string? message = null, Exception? inner = null) : base(message ?? $"{schema.GetType()} is not supported.", inner)
        {
            UnsupportedSchema = schema;
        }
    }
}
