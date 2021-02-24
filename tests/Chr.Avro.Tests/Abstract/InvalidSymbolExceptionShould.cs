namespace Chr.Avro.Tests
{
    using Chr.Avro.Abstract;
    using Xunit;

    public class InvalidSymbolExceptionShould
    {
        [Fact]
        public void IncludeSymbolInMessage()
        {
            var symbol = "INVALID.";
            var exception = new InvalidSymbolException(symbol);

            Assert.StartsWith($"\"{symbol}\"", exception.Message);
            Assert.Equal(symbol, exception.Symbol);
        }
    }
}
