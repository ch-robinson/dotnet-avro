namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build binary Avro serializers for specific
    /// <see cref="Type" />-<see cref="Schema" /> pairs.
    /// </summary>
    public interface IBinarySerializerBuilderCase : ISerializerBuilderCase<BinarySerializerBuilderContext, BinarySerializerBuilderCaseResult>
    {
    }
}
