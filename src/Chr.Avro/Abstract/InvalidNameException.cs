using System;

namespace Chr.Avro.Abstract
{
    /// <summary>
    /// An exception thrown when a name does not conform to the Avro naming rules.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#names">Avro spec</a>
    /// for the full naming rules.
    /// </remarks>
    [Serializable]
    public class InvalidNameException : Exception
    {
        /// <summary>
        /// The name that caused the exception to be thrown.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Creates an exception describing an invalid name.
        /// </summary>
        /// <param name="name">
        /// The invalid name.
        /// </param>
        public InvalidNameException(string name) : base($"\"{name}\" is not a valid Avro name.")
        {
            Name = name;
        }
    }
}
