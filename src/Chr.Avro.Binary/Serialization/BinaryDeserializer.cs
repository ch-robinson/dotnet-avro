namespace Chr.Avro.Serialization
{
    using System;

    /// <summary>
    /// A function that deserializes a .NET object from a binary-encoded Avro value.
    /// </summary>
    /// <typeparam name="T">
    /// The <see cref="Type" /> of object to be deserialized.
    /// </typeparam>
    /// <param name="reader">
    /// A <see cref="BinaryReader" /> around the encoded Avro data.
    /// </param>
    /// <returns>
    /// A deserialized object.
    /// </returns>
    public delegate T BinaryDeserializer<T>(ref BinaryReader reader);
}
