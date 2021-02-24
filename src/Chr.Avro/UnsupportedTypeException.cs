namespace Chr.Avro
{
    using System;

    /// <summary>
    /// An exception thrown when an operation does not support a .NET type.
    /// </summary>
    [Serializable]
    public class UnsupportedTypeException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UnsupportedTypeException" /> class.
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
        public UnsupportedTypeException(Type type, string? message = null, Exception? inner = null)
            : base(message ?? $"Failed to operate on {type.FullName}.", inner)
        {
            UnsupportedType = type ?? throw new ArgumentNullException(nameof(type), "Type cannot be null.");
        }

        /// <summary>
        /// Gets the <see cref="Type" /> that caused the exception to be thrown.
        /// </summary>
        public Type UnsupportedType { get; }
    }
}
