using System;

namespace Chr.Avro
{
    /// <summary>
    /// An exception thrown when an operation does not support a .NET type.
    /// </summary>
    [Serializable]
    public class UnsupportedTypeException : Exception
    {
        /// <summary>
        /// The type that caused the exception to be thrown.
        /// </summary>
        public Type UnsupportedType { get; }

        /// <summary>
        /// Creates an exception describing the error.
        /// </summary>
        /// <param name="type">
        /// The type that caused the exception to be thrown.
        /// </param>
        /// <param name="message">
        /// A message describing the exception.
        /// </param>
        /// <param name="inner">
        /// An underlying error that may provide additional context.
        /// </param>
        public UnsupportedTypeException(Type type, string? message = null, Exception? inner = null) : base(message ?? $"{type.FullName} is not supported.", inner)
        {
            UnsupportedType = type;
        }
    }
}
