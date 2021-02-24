namespace Chr.Avro
{
    using System;

    /// <summary>
    /// The exception thrown when a deserializer encounters invalid Avro data.
    /// </summary>
    [Serializable]
    public class InvalidEncodingException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidEncodingException" /> class.
        /// </summary>
        /// <param name="position">
        /// The position in the data at which the exception was thrown.
        /// </param>
        /// <param name="message">
        /// A message describing the exception.
        /// </param>
        /// <param name="inner">
        /// An underlying exception that may provide additional context.
        /// </param>
        public InvalidEncodingException(long position, string message, Exception? inner = null)
            : base(message, inner)
        {
            Position = position;
        }

        /// <summary>
        /// Gets the position in the data at which the exception was thrown.
        /// </summary>
        public long Position { get; private set; }
    }
}
