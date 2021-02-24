namespace Chr.Avro.Tests
{
    using Chr.Avro.Abstract;
    using Xunit;

    public class InvalidNameExceptionShould
    {
        [Fact]
        public void IncludeNameInMessage()
        {
            var name = "bad!";
            var exception = new InvalidNameException(name);

            Assert.StartsWith($"\"{name}\"", exception.Message);
            Assert.Equal(name, exception.Name);
        }
    }
}
