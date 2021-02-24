namespace Chr.Avro.Serialization
{
    using System;

    /// <summary>
    /// A function that serializes a .NET object to a binary-encoded Avro value.
    /// </summary>
    /// <typeparam name="T">
    /// The <see cref="Type" /> of object to be serialized.
    /// </typeparam>
    /// <param name="value">
    /// An unserialized value.
    /// </param>
    /// <param name="writer">
    /// A <see cref="BinaryWriter" /> around the output stream.
    /// </param>
    public delegate void BinarySerializer<T>(T value, BinaryWriter writer);
}
