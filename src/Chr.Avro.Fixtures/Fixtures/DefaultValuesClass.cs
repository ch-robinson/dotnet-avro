#pragma warning disable SA1401 // allow public fields

namespace Chr.Avro.Fixtures
{
    using System.ComponentModel;

    public class DefaultValuesClass
    {
        [DefaultValue(1)]
        public int DefaultIntField;

        [DefaultValue(null)]
        public object DefaultObjectField;

        [DefaultValue(1)]
        public int DefaultIntProperty { get; set; }

        [DefaultValue(null)]
        public object DefaultObjectProperty { get; set; }
    }
}
