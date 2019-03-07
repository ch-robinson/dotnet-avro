using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Moq;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Xunit;

using ISchemaRegistryClient = Confluent.SchemaRegistry.ISchemaRegistryClient;

namespace Chr.Avro.Confluent.Tests
{
    public class AsyncSchemaRegistrySerializerTests
    {
        protected readonly Mock<ISchemaRegistryClient> RegistryClientMock;

        public AsyncSchemaRegistrySerializerTests()
        {
            RegistryClientMock = new Mock<ISchemaRegistryClient>();
        }

        [Fact]
        public async Task CachesGeneratedSerializers()
        {
            var serializer = new AsyncSchemaRegistrySerializer<object>(
                RegistryClientMock.Object
            );

            var metadata = new MessageMetadata();
            var destination = new TopicPartition("test_topic", new Partition(0));
            var subject = $"{destination.Topic}-value";

            RegistryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new Schema(subject, 1, 12, "\"null\""));

            await Task.WhenAll(Enumerable.Range(0, 5).Select(i =>
                serializer.SerializeAsync(null, false, metadata, destination)
            ));

            RegistryClientMock
                .Verify(c => c.GetLatestSchemaAsync(subject), Times.Once());
        }

        [Fact]
        public async Task ProvidesDefaultSerializationComponents()
        {
            var serializer = new AsyncSchemaRegistrySerializer<int>(
                RegistryClientMock.Object
            );

            var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x04, 0x08 };
            var metadata = new MessageMetadata();
            var destination = new TopicPartition("test_topic", new Partition(0));
            var subject = $"{destination.Topic}-key";
            var value = 4;

            RegistryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new Schema(subject, 1, 4, "\"int\""));

            Assert.Equal(data, await serializer.SerializeAsync(value, true, metadata, destination));
        }

        [Fact]
        public async Task RegistersWhenSchemaIncompatible()
        {
            var serializer = new AsyncSchemaRegistrySerializer<int>(
                RegistryClientMock.Object,
                registerAutomatically: true
            );

            var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x0a, 0x0c };
            var metadata = new MessageMetadata();
            var destination = new TopicPartition("test_topic", new Partition(0));
            var subject = $"{destination.Topic}-value";
            var value = 6;

            RegistryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ReturnsAsync(new Schema(subject, 1, 9, "\"string\""));

            RegistryClientMock
                .Setup(c => c.RegisterSchemaAsync(subject, It.IsAny<string>()))
                .ReturnsAsync(10);

            Assert.Equal(data, await serializer.SerializeAsync(value, false, metadata, destination));
        }

        [Fact]
        public async Task RegistersWhenSubjectMissing()
        {
            var serializer = new AsyncSchemaRegistrySerializer<int>(
                RegistryClientMock.Object,
                registerAutomatically: true
            );

            var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x08, 0x0c };
            var metadata = new MessageMetadata();
            var destination = new TopicPartition("test_topic", new Partition(0));
            var subject = $"{destination.Topic}-value";
            var value = 6;

            RegistryClientMock
                .Setup(c => c.GetLatestSchemaAsync(subject))
                .ThrowsAsync(new SchemaRegistryException("Subject not found", HttpStatusCode.NotFound, 40401));

            RegistryClientMock
                .Setup(c => c.RegisterSchemaAsync(subject, It.IsAny<string>()))
                .ReturnsAsync(8);

            Assert.Equal(data, await serializer.SerializeAsync(value, false, metadata, destination));
        }
    }
}
