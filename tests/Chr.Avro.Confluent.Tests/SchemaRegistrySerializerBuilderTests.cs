using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Moq;
using System.Threading.Tasks;
using Xunit;

namespace Chr.Avro.Confluent.Tests
{
    public class SchemaRegistrySerializerBuilderTests
    {
        protected readonly Mock<ISchemaRegistryClient> RegistryMock;

        public SchemaRegistrySerializerBuilderTests()
        {
            RegistryMock = new Mock<ISchemaRegistryClient>(MockBehavior.Strict);
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaId()
        {
            var id = 6;
            var json = @"[""null"",""int""]";

            RegistryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                await builder.Build<int?>(id);

                RegistryMock.Verify();
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaSubject()
        {
            var id = 12;
            var json = @"""string""";
            var subject = "test-subject";
            var version = 4;

            RegistryMock.Setup(r => r.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, version, id, json, SchemaType.Avro, null))
                .Verifiable();

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                await builder.Build<string>(subject);

                RegistryMock.Verify();
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaSubjectAndVersion()
        {
            var id = 12;
            var json = @"""string""";
            var subject = "test-subject";
            var version = 4;

            RegistryMock.Setup(r => r.GetRegisteredSchemaAsync(subject, version))
                .ReturnsAsync(new RegisteredSchema(subject, version, id, json, SchemaType.Avro, null))
                .Verifiable();

            RegistryMock.Setup(r => r.GetSchemaIdAsync(subject, It.Is<Schema>(s => s.SchemaString == json)))
                .ReturnsAsync(id)
                .Verifiable();

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                await builder.Build<string>(subject, version);

                RegistryMock.Verify();
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00 }, "")]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x06, 0x73, 0x75, 0x70 }, "sup")]
        public async Task SerializesUsingConfluentWireFormat(byte[] encoding, string data)
        {
            var id = 12;
            var json = @"""string""";

            RegistryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            var context = new SerializationContext(MessageComponentType.Value, "test-topic");

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                var serializer = await builder.Build<string>(id);

                Assert.Equal(encoding, serializer.Serialize(data, context));
            }
        }

        [Fact]
        public async Task SerializesWithAutoRegistrationAlways()
        {
            var id = 40;
            var subject = "new_subject";

            RegistryMock.Setup(r => r.RegisterSchemaAsync(subject, It.Is<Schema>(s => s.SchemaType == SchemaType.Avro)))
                .ReturnsAsync(id)
                .Verifiable();

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                await builder.Build<string>(subject, registerAutomatically: AutomaticRegistrationBehavior.Always);

                RegistryMock.Verify(r => r.GetLatestSchemaAsync(subject), Times.Never());
                RegistryMock.Verify(r => r.RegisterSchemaAsync(subject, It.IsAny<Schema>()), Times.Once());
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Fact]
        public async Task SerializesWithAutoRegistrationNever()
        {
            var subject = "test-subject";

            RegistryMock.Setup(r => r.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new RegisteredSchema(subject, 1, 38, "\"int\"", SchemaType.Avro, null))
                .Verifiable();

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                await Assert.ThrowsAsync<UnsupportedTypeException>(() => builder.Build<string>(subject, registerAutomatically: AutomaticRegistrationBehavior.Never));

                RegistryMock.Verify();
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Fact]
        public async Task ThrowsOnInvalidTombstoneType()
        {
            var id = 4;
            var json = @"""int""";

            RegistryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
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

            RegistryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
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

            RegistryMock.Setup(r => r.GetSchemaAsync(id, null))
                .ReturnsAsync(new Schema(json, SchemaType.Avro))
                .Verifiable();

            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                await Assert.ThrowsAsync<UnsupportedSchemaException>(
                    () => builder.Build<int?>(id, TombstoneBehavior.Strict));
            }
        }
    }
}
