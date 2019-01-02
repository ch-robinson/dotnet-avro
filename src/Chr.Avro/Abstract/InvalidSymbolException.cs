using System;

namespace Chr.Avro.Abstract
{
    /// <summary>
    /// An exception thrown when a symbol does not conform to the Avro naming rules.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#names">Avro spec</a>
    /// for the full naming rules. (Symbols must conform to the same requirements as names.)
    /// </remarks>
    [Serializable]
    public class InvalidSymbolException : Exception
    {
        /// <summary>
        /// The symbol that caused the exception to be thrown.
        /// </summary>
        public string Symbol { get; private set; }

        /// <summary>
        /// Creates an exception describing an invalid symbol.
        /// </summary>
        /// <param name="symbol">
        /// The invalid symbol.
        /// </param>
        public InvalidSymbolException(string symbol) : base($"\"{symbol}\" is not a valid Avro enum symbol.")
        {
            Symbol = symbol;
        }
    }
}
