using Confluent.SchemaRegistry;
using Moq;
using System.Threading.Tasks;
using Xunit;

namespace Chr.Avro.Confluent.Tests
{
    public class SchemaRegistrySerializerBuilderTests
    {
        protected const string TestSubject = "test_subject";

        protected const int TestSubjectLatestId = 12;

        protected const string TestSubjectLatestString = @"""string""";

        protected const int TestSubjectLatestVersion = 4;

        protected const string TestTopic = "test_topic";

        protected readonly Mock<ISchemaRegistryClient> RegistryMock;

        public SchemaRegistrySerializerBuilderTests()
        {
            RegistryMock = new Mock<ISchemaRegistryClient>(MockBehavior.Strict);

            RegistryMock.Setup(r => r.GetLatestSchemaAsync(TestSubject))
                .ReturnsAsync(new Schema(
                    TestSubject,
                    TestSubjectLatestVersion,
                    TestSubjectLatestId,
                    TestSubjectLatestString
                ));

            RegistryMock.Setup(r => r.GetSchemaAsync(TestSubject, TestSubjectLatestVersion))
                .ReturnsAsync(TestSubjectLatestString);

            RegistryMock.Setup(r => r.GetSchemaAsync(TestSubjectLatestId))
                .ReturnsAsync(TestSubjectLatestString);

            RegistryMock.Setup(r => r.GetSchemaIdAsync(TestSubject, TestSubjectLatestString))
                .ReturnsAsync(TestSubjectLatestId);
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaId()
        {
            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                var serialize = await builder.BuildSerializer<string>(TestSubjectLatestId);

                RegistryMock.Verify(r => r.GetSchemaAsync(TestSubjectLatestId), Times.Once());
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaSubject()
        {
            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                var serialize = await builder.BuildSerializer<string>(TestSubject);

                RegistryMock.Verify(r => r.GetLatestSchemaAsync(TestSubject), Times.Once());
                RegistryMock.VerifyNoOtherCalls();
            }
        }

        [Fact]
        public async Task BuildsSerializerWithSchemaSubjectAndVersion()
        {
            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                var serialize = await builder.BuildSerializer<string>(TestSubject, TestSubjectLatestVersion);

                RegistryMock.Verify(r => r.GetSchemaAsync(TestSubject, TestSubjectLatestVersion), Times.Once());
                RegistryMock.Verify(r => r.GetSchemaIdAsync(TestSubject, TestSubjectLatestString), Times.Once());
            }
        }

        [Theory]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00 }, "")]
        [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0c, 0x06, 0x73, 0x75, 0x70 }, "sup")]
        public async Task SerializesUsingConfluentWireFormat(byte[] data, string encoding)
        {
            using (var builder = new SchemaRegistrySerializerBuilder(RegistryMock.Object))
            {
                var serialize = await builder.BuildSerializer<string>(TestSubjectLatestId);

                Assert.Equal(data, serialize(TestTopic, encoding));
            }
        }
    }
}
