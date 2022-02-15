namespace Chr.Avro
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// An exception thrown when an operation does not support a schema.
    /// </summary>
    [Serializable]
    public class UnsupportedSchemaException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UnsupportedSchemaException" /> class.
        /// </summary>
        /// <param name="schema">
        /// The <see cref="Schema" />  that caused the exception to be thrown.
        /// </param>
        /// <param name="message">
        /// A message describing the exception.
        /// </param>
        /// <param name="inner">
        /// An underlying exception that may provide additional context.
        /// </param>
        public UnsupportedSchemaException(Schema schema, string? message = null, Exception? inner = null)
            : base(message ?? $"Failed to operate on {schema.GetType().FullName}.", inner)
        {
            UnsupportedSchema = schema ?? throw new ArgumentNullException(nameof(schema), "Schema cannot be null.");
        }

        /// <summary>
        /// Gets the <see cref="Schema" /> that caused the exception to be thrown.
        /// </summary>
        public Schema UnsupportedSchema { get; }
    }
}
