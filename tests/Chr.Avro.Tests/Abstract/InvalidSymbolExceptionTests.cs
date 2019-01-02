using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Tests
{
    public class InvalidSymbolExceptionTests
    {
        [Fact]
        public void IncludesSymbolInMessage()
        {
            var symbol = "INVALID.";
            var exception = new InvalidSymbolException(symbol);

            Assert.StartsWith($"\"{symbol}\"", exception.Message);
            Assert.Equal(symbol, exception.Symbol);
        }
    }
}
