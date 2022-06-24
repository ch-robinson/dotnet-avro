namespace Chr.Avro.Fixtures
{
    using System.ComponentModel.DataAnnotations;

    public class RangeAnnotatedPropertiesClass
    {
        [Range(typeof(decimal), "0.00", "999999.99", ConvertValueInInvariantCulture = true)]
        public decimal Currency { get; set; }

        [Range(typeof(decimal?), "0.00", "999999.99", ConvertValueInInvariantCulture = true)]
        public decimal? NullableCurrency { get; set; }

        [Range(typeof(decimal), "-.500", ".500", ConvertValueInInvariantCulture = true)]
        public decimal FractionOnly { get; set; }

        [Range(typeof(decimal), "-5", "5", ConvertValueInInvariantCulture = true)]
        public decimal WholeOnly { get; set; }

        [Range(0.0, 1.0)]
        public decimal DoubleBounded { get; set; }
    }
}
