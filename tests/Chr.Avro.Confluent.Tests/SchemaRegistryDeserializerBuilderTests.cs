using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Moq;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Chr.Avro.Confluent.Tests
{
    public class SchemaRegistryDeserializerBuilderTests
    {
        protected readonly Mock<ISchemaRegistryClient> RegistryMock;

        public SchemaRegistryDeserializerBuilderTests()
        {
            RegistryMock = new Mock<ISchemaRegistryClient>(MockBehavior.Strict);
        }

        [Fact]
        public async Task BuildsDeserializerWithSchemaId()
        {
            var id = 6;
            var json = @"[""null"",""int""]";

            RegistryMock.Setup(r => r.GetSchemaAsync(id))
                .ReturnsAsync(json)
                .Verifiable();

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                await builder.Build<int?>(id);

                RegistryMock.Verify();
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Fact]
        public async Task BuildsDeserializerWithSchemaSubject()
        {
            var id = 12;
            var json = @"""string""";
            var subject = "test-subject";
            var version = 4;

            RegistryMock.Setup(r => r.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new Schema(subject, version, id, json))
                .Verifiable();

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                await builder.Build<string>(subject);

                RegistryMock.Verify();
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Fact]
        public async Task BuildsDeserializerWithSchemaSubjectAndVersion()
        {
            var id = 12;
            var json = @"""string""";
            var subject = "test-subject";
            var version = 4;

            RegistryMock.Setup(r => r.GetSchemaAsync(subject, version))
                .ReturnsAsync(json)
                .Verifiable();

            RegistryMock.Setup(r => r.GetSchemaIdAsync(subject, json))
                .ReturnsAsync(id)
                .Verifiable();

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                await builder.Build<string>(subject, version);

                RegistryMock.Verify();
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00 }, "")]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x06, 0x73, 0x75, 0x70 }, "sup")]
        public async Task DeserializesConfluentWireFormat(byte[] encoding, string data)
        {
            var id = 12;
            var json = @"""string""";

            RegistryMock.Setup(r => r.GetSchemaAsync(id))
                .ReturnsAsync(json)
                .Verifiable();

            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                var deserializer = await builder.Build<string>(id);

                Assert.Equal(data, deserializer.Deserialize(encoding, false, context));
            }
        }

        [Fact]
        public async Task ThrowsOnInvalidTombstoneType()
        {
            var id = 4;
            var json = @"""int""";

            RegistryMock.Setup(r => r.GetSchemaAsync(id))
                .ReturnsAsync(json)
                .Verifiable();

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                await Assert.ThrowsAsync<UnsupportedTypeException>(
                    () => builder.Build<int>(id, TombstoneBehavior.Strict));
            }
        }

        [Fact]
        public async Task ThrowsOnNullTombstoneSchema()
        {
            var id = 1;
            var json = @"""null""";

            RegistryMock.Setup(r => r.GetSchemaAsync(id))
                .ReturnsAsync(json)
                .Verifiable();

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                await Assert.ThrowsAsync<UnsupportedSchemaException>(
                    () => builder.Build<int?>(id, TombstoneBehavior.Strict));
            }
        }

        [Fact]
        public async Task ThrowsOnNullableTombstoneSchema()
        {
            var id = 6;
            var json = @"[""null"",""int""]";

            RegistryMock.Setup(r => r.GetSchemaAsync(id))
                .ReturnsAsync(json)
                .Verifiable();

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                await Assert.ThrowsAsync<UnsupportedSchemaException>(
                    () => builder.Build<int?>(id, TombstoneBehavior.Strict));
            }
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0b, 0x00 })]
        public async Task ThrowsOnSchemaIdMismatch(byte[] encoding)
        {
            var id = 12;
            var json = @"""string""";

            RegistryMock.Setup(r => r.GetSchemaAsync(id))
                .ReturnsAsync(json)
                .Verifiable();

            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                var deserializer = await builder.Build<string>(id);

                Assert.Throws<InvalidDataException>(
                    () => deserializer.Deserialize(encoding, false, context));
            }
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00 })]
        [InlineData(new byte[] { 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00 })]
        public async Task ThrowsOnUnrecognizedWireFormat(byte[] encoding)
        {
            var id = 12;
            var json = @"""string""";

            RegistryMock.Setup(r => r.GetSchemaAsync(id))
                .ReturnsAsync(json)
                .Verifiable();

            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using (var builder = new SchemaRegistryDeserializerBuilder(RegistryMock.Object))
            {
                var deserializer = await builder.Build<string>(id);

                Assert.Throws<InvalidDataException>(() =>
                    deserializer.Deserialize(encoding, false, context));
            }
        }
    }
}
