using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Moq;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

using ISchemaRegistryClient = Confluent.SchemaRegistry.ISchemaRegistryClient;

namespace Chr.Avro.Confluent.Tests
{
    public class AsyncSchemaRegistryDeserializerTests
    {
        protected readonly Mock<ISchemaRegistryClient> RegistryClientMock;

        public AsyncSchemaRegistryDeserializerTests()
        {
            RegistryClientMock = new Mock<ISchemaRegistryClient>();
        }

        [Fact]
        public async Task CachesGeneratedDeserializers()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<object>(
                RegistryClientMock.Object
            );

            var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
            var metadata = new MessageMetadata();
            var source = new TopicPartition("test_topic", new Partition(0));

            RegistryClientMock
                .Setup(c => c.GetSchemaAsync(0))
                .ReturnsAsync("\"null\"");

            await Task.WhenAll(Enumerable.Range(0, 5).Select(i =>
                deserializer.DeserializeAsync(data, false, false, metadata, source)
            ));

            RegistryClientMock
                .Verify(c => c.GetSchemaAsync(0), Times.Once());
        }

        [Fact]
        public async Task ProvidesDefaultDeserializationComponents()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<int>(
                RegistryClientMock.Object
            );

            var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x04, 0x08 };
            var metadata = new MessageMetadata();
            var source = new TopicPartition("test_topic", new Partition(0));
            var value = 4;

            RegistryClientMock
                .Setup(c => c.GetSchemaAsync(4))
                .ReturnsAsync("\"int\"");

            Assert.Equal(value, await deserializer.DeserializeAsync(data, false, false, metadata, source));
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00 })]
        [InlineData(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00 })]
        public async Task ThrowsOnUnrecognizedWireFormat(byte[] data)
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<int>(
                RegistryClientMock.Object
            );

            var metadata = new MessageMetadata();
            var source = new TopicPartition("test_topic", new Partition(0));

            await Assert.ThrowsAsync<InvalidDataException>(() =>
                deserializer.DeserializeAsync(data, false, false, metadata, source)
            );
        }
    }
}
