namespace Chr.Avro.Fixtures
{
    using System.ComponentModel.DataAnnotations;

    public class RangeAnnotatedPropertiesClass
    {
#if NET6_0_OR_GREATER
        [Range(typeof(decimal), "0.00", "999999.99", ConvertValueInInvariantCulture = true)]
#else
        [Range(typeof(decimal), "0.00", "999999.99")]
#endif
        public decimal Currency { get; set; }

#if NET6_0_OR_GREATER
        [Range(typeof(decimal?), "0.00", "999999.99", ConvertValueInInvariantCulture = true)]
#else
        [Range(typeof(decimal?), "0.00", "999999.99")]
#endif
        public decimal? NullableCurrency { get; set; }

#if NET6_0_OR_GREATER
        [Range(typeof(decimal), "-.500", ".500", ConvertValueInInvariantCulture = true)]
#else
        [Range(typeof(decimal), "-.500", ".500")]
#endif
        public decimal FractionOnly { get; set; }

        [Range(typeof(decimal), "-5", "5")]
        public decimal WholeOnly { get; set; }

        [Range(0.0, 1.0)]
        public decimal DoubleBounded { get; set; }
    }
}
