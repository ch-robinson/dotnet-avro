namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build JSON Avro serializers for specific
    /// <see cref="Type" />-<see cref="Schema" /> pairs.
    /// </summary>
    public interface IJsonSerializerBuilderCase : ISerializerBuilderCase<JsonSerializerBuilderContext, JsonSerializerBuilderCaseResult>
    {
    }
}
