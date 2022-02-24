namespace Chr.Avro.Serialization
{
    using System;
    using System.Text.Json;

    /// <summary>
    /// A function that serializes a .NET object to a JSON-encoded Avro value.
    /// </summary>
    /// <typeparam name="T">
    /// The <see cref="Type" /> of object to be serialized.
    /// </typeparam>
    /// <param name="value">
    /// An unserialized value.
    /// </param>
    /// <param name="writer">
    /// A <see cref="Utf8JsonWriter" /> around the output stream.
    /// </param>
    public delegate void JsonSerializer<T>(T value, Utf8JsonWriter writer);
}
