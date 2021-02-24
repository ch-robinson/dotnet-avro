namespace Chr.Avro.Serialization
{
    using System;
    using System.Text.Json;

    /// <summary>
    /// A function that deserializes a .NET object from a JSON-encoded Avro value.
    /// </summary>
    /// <typeparam name="T">
    /// The <see cref="Type" /> of object to be deserialized.
    /// </typeparam>
    /// <param name="reader">
    /// A <see cref="Utf8JsonReader" /> around the encoded Avro data.
    /// </param>
    /// <returns>
    /// A deserialized object.
    /// </returns>
    public delegate T JsonDeserializer<T>(ref Utf8JsonReader reader);
}
