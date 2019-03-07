using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Moq;
using Xunit;

using ISchemaRegistryClient = Confluent.SchemaRegistry.ISchemaRegistryClient;

namespace Chr.Avro.Confluent.Tests
{
    public class AsyncSchemaRegistryDeserializerTests
    {
        protected readonly Mock<IBinaryDeserializerBuilder> DeserializerBuilderMock;

        protected readonly Mock<ISchemaRegistryClient> RegistryClientMock;

        protected readonly Mock<IJsonSchemaReader> SchemaReaderMock;

        protected readonly AsyncSchemaRegistryDeserializer<int> Deserializer;

        public AsyncSchemaRegistryDeserializerTests()
        {
            DeserializerBuilderMock = new Mock<IBinaryDeserializerBuilder>();
            RegistryClientMock = new Mock<ISchemaRegistryClient>();
            SchemaReaderMock = new Mock<IJsonSchemaReader>();

            Deserializer = new AsyncSchemaRegistryDeserializer<int>(
                RegistryClientMock.Object,
                DeserializerBuilderMock.Object,
                SchemaReaderMock.Object
            );
        }

        [Fact]
        public async Task CachesGeneratedDeserializers()
        {
            var data = new byte[5];
            var metadata = new MessageMetadata();
            var tp = new TopicPartition("test_topic", new Partition(0));

            DeserializerBuilderMock
                .Setup(b => b.BuildDelegate<int>(It.IsAny<Schema>(), null))
                .Returns(stream =>
                {
                    stream.Position = stream.Length;
                    return 0;
                });

            await Task.WhenAll(Enumerable.Range(0, 5).Select(i =>
                Deserializer.DeserializeAsync(data, false, false, metadata, tp)
            ));

            DeserializerBuilderMock
                .Verify(b => b.BuildDelegate<int>(It.IsAny<Schema>(), null), Times.Once());
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00 })]
        [InlineData(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00 })]
        public async Task ThrowsOnUnrecognizedWireFormat(byte[] data)
        {
            var metadata = new MessageMetadata();
            var tp = new TopicPartition("test_topic", new Partition(0));

            await Assert.ThrowsAsync<InvalidDataException>(() =>
                Deserializer.DeserializeAsync(data, false, false, metadata, tp)
            );
        }
    }
}
