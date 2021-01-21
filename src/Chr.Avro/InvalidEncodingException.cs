using System;

namespace Chr.Avro
{
    /// <summary>
    /// An exception thrown when a deserializer encounters invalid Avro data.
    /// </summary>
    [Serializable]
    public class InvalidEncodingException : Exception
    {
        /// <summary>
        /// The index at which the error occurred.
        /// </summary>
        public long Position { get; private set; }

        /// <summary>
        /// Creates an exception describing the error.
        /// </summary>
        /// <param name="position">
        /// The position at which the error occurred.
        /// </param>
        /// <param name="message">
        /// A message describing the exception.
        /// </param>
        /// <param name="inner">
        /// An underlying error that may provide additional context.
        /// </param>
        public InvalidEncodingException(long position, string message, Exception? inner = null) : base(message, inner)
        {
            Position = position;
        }
    }
}
