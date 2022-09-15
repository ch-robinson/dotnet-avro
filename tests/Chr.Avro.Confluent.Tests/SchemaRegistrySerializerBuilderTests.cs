namespace Chr.Avro.Confluent.Tests
{
    using System.Threading.Tasks;
    using global::Confluent.Kafka;
    using global::Confluent.SchemaRegistry;
    using Moq;
    using Xunit;

    public class SchemaRegistrySerializerBuilderTests
    {
        private readonly Mock<ISchemaRegistryClient> registryMock;

        public SchemaRegistrySerializerBuilderTests()
        {
            registryMock = new Mock<ISchemaRegistryClient>(MockBehavior.Strict);
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaId()
        {
            var id = 6;
            var json = @"[""null"",""int""]";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

            await builder.Build<int?>(id);

            registryMock.Verify();
            registryMock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaSubject()
        {
            var id = 12;
            var json = @"""string""";
            var subject = "test-subject";
            var version = 4;

            registryMock.Setup(r => r.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, version, id, json, SchemaType.Avro, null))
                .Verifiable();

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

            await builder.Build<string>(subject);

            registryMock.Verify();
            registryMock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaSubjectAndVersion()
        {
            var id = 12;
            var json = @"""string""";
            var subject = "test-subject";
            var version = 4;

            registryMock.Setup(r => r.GetRegisteredSchemaAsync(subject, version))
                .ReturnsAsync(new RegisteredSchema(subject, version, id, json, SchemaType.Avro, null))
                .Verifiable();

            registryMock.Setup(r => r.GetSchemaIdAsync(subject, It.Is<Schema>(s => s.SchemaString == json), false))
                .ReturnsAsync(id)
                .Verifiable();

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

            await builder.Build<string>(subject, version);

            registryMock.Verify();
            registryMock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task HandlesConfluentWireFormatBytesCase()
        {
            var id = 0;
            var json = @"""bytes""";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            var data = new byte[] { 0x02 };
            var encoding = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x02 };
            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

            var serializer = await builder.Build<byte[]>(id);

            Assert.Equal(encoding, serializer.Serialize(data, context));
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00 }, "")]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x06, 0x73, 0x75, 0x70 }, "sup")]
        public async Task SerializesUsingConfluentWireFormat(byte[] encoding, string data)
        {
            var id = 12;
            var json = @"""string""";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

            var serializer = await builder.Build<string>(id);

            Assert.Equal(encoding, serializer.Serialize(data, context));
        }

        [Fact]
        public async Task SerializesWithAutoRegistrationAlways()
        {
            var id = 40;
            var subject = "new_subject";

            registryMock.Setup(r => r.RegisterSchemaAsync(subject, It.Is<Schema>(s => s.SchemaType == SchemaType.Avro), false))
                .ReturnsAsync(id)
                .Verifiable();

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

            await builder.Build<string>(subject, registerAutomatically: AutomaticRegistrationBehavior.Always);

            registryMock.Verify(r => r.GetLatestSchemaAsync(subject), Times.Never());
            registryMock.Verify(r => r.RegisterSchemaAsync(subject, It.IsAny<Schema>(), false), Times.Once());
            registryMock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task SerializesWithAutoRegistrationNever()
        {
            var subject = "test-subject";

            registryMock.Setup(r => r.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 38, "\"int\"", SchemaType.Avro, null))
                .Verifiable();

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

            await Assert.ThrowsAsync<UnsupportedTypeException>(() => builder.Build<string>(subject, registerAutomatically: AutomaticRegistrationBehavior.Never));

            registryMock.Verify();
            registryMock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task ThrowsOnInvalidTombstoneType()
        {
            var id = 4;
            var json = @"""int""";

            registryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

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

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

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

            using var builder = new SchemaRegistrySerializerBuilder(registryMock.Object);

            await Assert.ThrowsAsync<UnsupportedSchemaException>(
                () => builder.Build<int?>(id, TombstoneBehavior.Strict));
        }
    }
}
