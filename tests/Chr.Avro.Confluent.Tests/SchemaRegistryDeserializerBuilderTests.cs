namespace Chr.Avro.Confluent.Tests
{
    using System.IO;
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using global::Confluent.SchemaRegistry;
    using Moq;
    using Xunit;

    public class SchemaRegistryDeserializerBuilderTests
    {
        private readonly Mock<ISchemaRegistryClient> registryMock;

        public SchemaRegistryDeserializerBuilderTests()
        {
            registryMock = new Mock<ISchemaRegistryClient>(MockBehavior.Strict);
        }

        [Fact]
        public async Task BuildsDeserializerWithSchemaId()
        {
            var id = 6;
            var json = @"[""null"",""int""]";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            await builder.Build<int?>(id);

            registryMock.Verify();
            registryMock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task BuildsDeserializerWithSchemaSubject()
        {
            var id = 12;
            var json = @"""string""";
            var subject = "test-subject";
            var version = 4;

            registryMock.Setup(r => r.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, version, id, json, SchemaType.Avro, null))
                .Verifiable();

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            await builder.Build<string>(subject);

            registryMock.Verify();
            registryMock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task BuildsDeserializerWithSchemaSubjectAndVersion()
        {
            var id = 12;
            var json = @"""string""";
            var subject = "test-subject";
            var version = 4;

            registryMock.Setup(r => r.GetRegisteredSchemaAsync(subject, version))
                .ReturnsAsync(new RegisteredSchema(subject, version, id, json, SchemaType.Avro, null))
                .Verifiable();

            registryMock.Setup(r => r.GetSchemaIdAsync(subject, It.Is<Schema>(s => s.SchemaString == json)))
                .ReturnsAsync(id)
                .Verifiable();

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            await builder.Build<string>(subject, version);

            registryMock.Verify();
            registryMock.VerifyNoOtherCalls();
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00 }, "")]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x06, 0x73, 0x75, 0x70 }, "sup")]
        public async Task DeserializesConfluentWireFormat(byte[] encoding, string data)
        {
            var id = 12;
            var json = @"""string""";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            var deserializer = await builder.Build<string>(id);

            Assert.Equal(data, deserializer.Deserialize(encoding, false, context));
        }

        [Fact]
        public async Task ThrowsOnInvalidTombstoneType()
        {
            var id = 4;
            var json = @"""int""";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            await Assert.ThrowsAsync<UnsupportedTypeException>(
                () => builder.Build<int>(id, TombstoneBehavior.Strict));
        }

        [Fact]
        public async Task ThrowsOnNullTombstoneSchema()
        {
            var id = 1;
            var json = @"""null""";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            await Assert.ThrowsAsync<UnsupportedSchemaException>(
                () => builder.Build<int?>(id, TombstoneBehavior.Strict));
        }

        [Fact]
        public async Task ThrowsOnNullableTombstoneSchema()
        {
            var id = 6;
            var json = @"[""null"",""int""]";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            await Assert.ThrowsAsync<UnsupportedSchemaException>(
                () => builder.Build<int?>(id, TombstoneBehavior.Strict));
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0b, 0x00 })]
        public async Task ThrowsOnSchemaIdMismatch(byte[] encoding)
        {
            var id = 12;
            var json = @"""string""";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            var deserializer = await builder.Build<string>(id);

            Assert.Throws<InvalidEncodingException>(
                () => deserializer.Deserialize(encoding, false, context));
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00 })]
        [InlineData(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00 })]
        public async Task ThrowsOnUnrecognizedWireFormat(byte[] encoding)
        {
            var id = 12;
            var json = @"""string""";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using var builder = new SchemaRegistryDeserializerBuilder(registryMock.Object);

            var deserializer = await builder.Build<string>(id);

            Assert.Throws<InvalidEncodingException>(() =>
                deserializer.Deserialize(encoding, false, context));
        }
    }
}
