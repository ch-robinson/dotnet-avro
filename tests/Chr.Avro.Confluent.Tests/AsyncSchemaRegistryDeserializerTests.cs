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

            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            RegistryClientMock
                .Setup(c => c.GetSchemaAsync(0))
                .ReturnsAsync("\"null\"");

            await Task.WhenAll(Enumerable.Range(0, 5).Select(i =>
                deserializer.DeserializeAsync(encoding, false, context)
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

            var data = 4;
            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x04, 0x08 };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            RegistryClientMock
                .Setup(c => c.GetSchemaAsync(4))
                .ReturnsAsync("\"int\"");

            Assert.Equal(data,
                await deserializer.DeserializeAsync(encoding, false, context)
            );
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00 })]
        [InlineData(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00 })]
        public async Task ThrowsOnUnrecognizedWireFormat(byte[] encoding)
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<int>(
                RegistryClientMock.Object
            );

            var metadata = new MessageMetadata();
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            await Assert.ThrowsAsync<InvalidDataException>(() =>
                deserializer.DeserializeAsync(encoding, false, context)
            );
        }
    }
}
