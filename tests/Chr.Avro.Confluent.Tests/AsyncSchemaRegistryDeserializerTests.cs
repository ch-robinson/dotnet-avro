namespace Chr.Avro.Confluent.Tests
{
    using System;
    using System.Linq;
    using System.Net.Http;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using global::Confluent.SchemaRegistry;
    using Moq;
    using Xunit;

    public class AsyncSchemaRegistryDeserializerTests
    {
        private readonly Mock<ISchemaRegistryClient> registryClientMock;

        public AsyncSchemaRegistryDeserializerTests()
        {
            registryClientMock = new Mock<ISchemaRegistryClient>();
        }

        [Fact]
        public async Task CachesGeneratedDeserializers()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<object>(
                registryClientMock.Object);

            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            registryClientMock
                .Setup(c => c.GetSchemaAsync(0, null))
                .ReturnsAsync(new Schema("\"null\"", SchemaType.Avro));

            await Task.WhenAll(Enumerable.Range(0, 5).Select(i =>
                deserializer.DeserializeAsync(encoding, encoding.Length == 0, context)));

            registryClientMock
                .Verify(c => c.GetSchemaAsync(0, null), Times.Once());
        }

        [Fact]
        public async Task DoesNotCacheSchemaRegistryFailures()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<object>(
                registryClientMock.Object);

            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            registryClientMock
                .Setup(c => c.GetSchemaAsync(0, null))
                .ThrowsAsync(new HttpRequestException());

            await Assert.ThrowsAsync<HttpRequestException>(() =>
                deserializer.DeserializeAsync(encoding, encoding.Length == 0, context));

            registryClientMock
                .Setup(c => c.GetSchemaAsync(0, null))
                .ReturnsAsync(new Schema("\"null\"", SchemaType.Avro));

            await deserializer.DeserializeAsync(encoding, encoding.Length == 0, context);
        }

        [Fact]
        public async Task HandlesConfluentWireFormatBytesCase()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<byte[]>(
                registryClientMock.Object);

            var data = new byte[] { 0x02 };
            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x02 };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            registryClientMock
                .Setup(c => c.GetSchemaAsync(0, null))
                .ReturnsAsync(new Schema("\"bytes\"", SchemaType.Avro));

            Assert.Equal(data, await deserializer.DeserializeAsync(encoding, false, context));
        }

        [Fact]
        public async Task ProvidesDefaultDeserializationComponents()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<int>(
                registryClientMock.Object);

            var data = 4;
            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x04, 0x08 };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            registryClientMock
                .Setup(c => c.GetSchemaAsync(4, null))
                .ReturnsAsync(new Schema("\"int\"", SchemaType.Avro));

            Assert.Equal(
                data,
                await deserializer.DeserializeAsync(encoding, encoding.Length == 0, context));
        }

        [Fact]
        public async Task ReturnsTombstone()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<object>(
                registryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict);

            var encoding = Array.Empty<byte>();
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            Assert.Null(await deserializer.DeserializeAsync(encoding, encoding.Length == 0, context));
        }

        [Fact]
        public async Task ThrowsOnInvalidTombstoneComponent()
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<object>(
                registryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict);

            var encoding = Array.Empty<byte>();
            var context = new SerializationContext(MessageComponentType.Key, "test_topic");

            await Assert.ThrowsAsync<InvalidEncodingException>(() =>
                deserializer.DeserializeAsync(encoding, encoding.Length == 0, context));
        }

        [Fact]
        public void ThrowsOnInvalidTombstoneType()
        {
            Assert.Throws<UnsupportedTypeException>(() => new AsyncSchemaRegistryDeserializer<int>(
                registryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict));
        }

        [Theory]
        [InlineData(new byte[] { })]
        [InlineData(new byte[] { 0x00, 0x00, 0x00 })]
        [InlineData(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00 })]
        public async Task ThrowsOnUnrecognizedWireFormat(byte[] encoding)
        {
            var deserializer = new AsyncSchemaRegistryDeserializer<int>(
                registryClientMock.Object);

            var context = new SerializationContext(MessageComponentType.Value, "test_topic");

            await Assert.ThrowsAsync<InvalidEncodingException>(() =>
                deserializer.DeserializeAsync(encoding, encoding.Length == 0, context));
        }
    }
}
