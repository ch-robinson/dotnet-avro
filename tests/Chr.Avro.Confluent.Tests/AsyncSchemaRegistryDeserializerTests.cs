using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Moq;
using System.IO;
using System.Linq;
using System.Net.Http;
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
                deserializer.DeserializeAsync(encoding, encoding.Length == 0, context)
            ));

            RegistryClientMock
                .Verify(c => c.GetSchemaAsync(0), Times.Once());
        }

        [Fact]
        public async Task DoesNotCacheSchemaRegistryFailures()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<object>(
                RegistryClientMock.Object
            );

            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            RegistryClientMock
                .Setup(c => c.GetSchemaAsync(0))
                .ThrowsAsync(new HttpRequestException());

            await Assert.ThrowsAsync<HttpRequestException>(() =>
                deserializer.DeserializeAsync(encoding, encoding.Length == 0, context)
            );

            RegistryClientMock
                .Setup(c => c.GetSchemaAsync(0))
                .ReturnsAsync("\"null\"");

            await deserializer.DeserializeAsync(encoding, encoding.Length == 0, context);
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
                await deserializer.DeserializeAsync(encoding, encoding.Length == 0, context)
            );
        }

        [Fact]
        public async Task ReturnsTombstone()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<object>(
                RegistryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict
            );

            var encoding = new byte[0];
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            Assert.Null(await deserializer.DeserializeAsync(encoding, encoding.Length == 0, context));
        }

        [Fact]
        public async Task ThrowsOnInvalidTombstoneComponent()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<object>(
                RegistryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict
            );

            var encoding = new byte[0];
            var context = new SerializationContext(MessageComponentType.Key, "test_topic");

            await Assert.ThrowsAsync<InvalidDataException>(() =>
                deserializer.DeserializeAsync(encoding, encoding.Length == 0, context)
            );
        }

        [Fact]
        public void ThrowsOnInvalidTombstoneType()
        {
            Assert.Throws<UnsupportedTypeException>(() => new AsyncSchemaRegistryDeserializer<int>(
                RegistryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict
            ));
        }

        [Theory]
        [InlineData(new byte[] { })]
        [InlineData(new byte[] { 0x00, 0x00, 0x00 })]
        [InlineData(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00 })]
        public async Task ThrowsOnUnrecognizedWireFormat(byte[] encoding)
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<int>(
                RegistryClientMock.Object
            );

            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            await Assert.ThrowsAsync<InvalidDataException>(() =>
                deserializer.DeserializeAsync(encoding, encoding.Length == 0, context)
            );
        }
    }
}
