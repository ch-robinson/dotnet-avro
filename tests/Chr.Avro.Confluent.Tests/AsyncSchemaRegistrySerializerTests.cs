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

    public class AsyncSchemaRegistrySerializerTests
    {
        private readonly Mock<ISchemaRegistryClient> registryClientMock;

        public AsyncSchemaRegistrySerializerTests()
        {
            registryClientMock = new Mock<ISchemaRegistryClient>();
        }

        [Fact]
        public async Task CachesGeneratedSerializers()
        {
            var serializer = new AsyncSchemaRegistrySerializer<object>(
                registryClientMock.Object);

            var context = new SerializationContext(MessageComponentType.Value, "test_topic");
            var subject = $"{context.Topic}-value";

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 12, "\"null\"", SchemaType.Avro, null));

            await Task.WhenAll(Enumerable.Range(0, 5).Select(i =>
                serializer.SerializeAsync(null, context)));

            registryClientMock
                .Verify(c => c.GetLatestSchemaAsync(subject), Times.Once());
        }

        [Fact]
        public async Task DoesNotCacheSchemaRegistryFailures()
        {
            var serializer = new AsyncSchemaRegistrySerializer<object>(
                registryClientMock.Object);

            var context = new SerializationContext(MessageComponentType.Value, "test_topic");
            var subject = $"{context.Topic}-value";

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ThrowsAsync(new HttpRequestException());

            await Assert.ThrowsAsync<HttpRequestException>(() =>
                serializer.SerializeAsync(null, context));

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 12, "\"null\"", SchemaType.Avro, null));

            await serializer.SerializeAsync(null, context);
        }

        [Fact]
        public async Task HandlesConfluentWireFormatBytesCase()
        {
            var serializer = new AsyncSchemaRegistrySerializer<object>(
                registryClientMock.Object);

            var data = new byte[] { 0x02 };
            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x02 };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");
            var subject = $"{context.Topic}-value";

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 0, "\"bytes\"", SchemaType.Avro, null));

            Assert.Equal(
                encoding,
                await serializer.SerializeAsync(data, context));
        }

        [Fact]
        public async Task ProvidesDefaultSerializationComponents()
        {
            var serializer = new AsyncSchemaRegistrySerializer<int>(
                registryClientMock.Object);

            var data = 4;
            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x04, 0x08 };
            var context = new SerializationContext(MessageComponentType.Key, "test_topic");
            var subject = $"{context.Topic}-key";

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 4, "\"int\"", SchemaType.Avro, null));

            Assert.Equal(
                encoding,
                await serializer.SerializeAsync(data, context));
        }

        [Fact]
        public async Task SerializesTombstone()
        {
            var serializer = new AsyncSchemaRegistrySerializer<object>(
                registryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict);

            var data = (int?)null;
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");
            var subject = $"{context.Topic}-value";

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 4, "\"int\"", SchemaType.Avro, null));

            Assert.Null(await serializer.SerializeAsync(data, context));
        }

        [Fact]
        public async Task SerializesWithAutoRegistrationAlways()
        {
            var serializer = new AsyncSchemaRegistrySerializer<int>(
                registryClientMock.Object,
                registerAutomatically: AutomaticRegistrationBehavior.Always);

            var data = 6;
            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x09, 0x0c };
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");
            var subject = $"{context.Topic}-value";

            registryClientMock
                .Setup(c => c.RegisterSchemaAsync(subject, It.Is<Schema>(s => s.SchemaType == SchemaType.Avro), false))
                .ReturnsAsync(9);

            Assert.Equal(encoding, await serializer.SerializeAsync(data, context));
        }

        [Fact]
        public async Task SerializesWithAutoRegistrationNever()
        {
            var serializer = new AsyncSchemaRegistrySerializer<int>(
                registryClientMock.Object,
                registerAutomatically: AutomaticRegistrationBehavior.Never);

            var data = 6;
            var context = new SerializationContext(MessageComponentType.Value, "test_topic");
            var subject = $"{context.Topic}-value";

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 9, "\"string\"", SchemaType.Avro, null));

            await Assert.ThrowsAsync<UnsupportedTypeException>(() => serializer.SerializeAsync(data, context));
        }

        [Fact]
        public async Task ThrowsOnInvalidTombstoneComponent()
        {
            var serializer = new AsyncSchemaRegistrySerializer<int?>(
                registryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict);

            var data = (int?)null;
            var context = new SerializationContext(MessageComponentType.Key, "test_topic");
            var subject = $"{context.Topic}-key";

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 4, "\"int\"", SchemaType.Avro, null));

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                serializer.SerializeAsync(data, context));
        }

        [Fact]
        public void ThrowsOnInvalidTombstoneType()
        {
            Assert.Throws<UnsupportedTypeException>(() => new AsyncSchemaRegistrySerializer<int>(
                registryClientMock.Object,
                tombstoneBehavior: TombstoneBehavior.Strict));
        }

        [Fact]
        public async Task UsesSubjectNameBuilder()
        {
            var version = GetType().Assembly.GetName().Version;

            var serializer = new AsyncSchemaRegistrySerializer<int>(
                registryClientMock.Object,
                subjectNameBuilder: c => $"{c.Topic}-{version}-{c.Component.ToString().ToLowerInvariant()}");

            var data = 2;
            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x08, 0x04 };
            var context = new SerializationContext(MessageComponentType.Key, "test_topic");
            var subject = $"{context.Topic}-{version}-key";

            registryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 2, 8, "\"int\"", SchemaType.Avro, null));

            Assert.Equal(
                encoding,
                await serializer.SerializeAsync(data, context));
        }
    }
}
