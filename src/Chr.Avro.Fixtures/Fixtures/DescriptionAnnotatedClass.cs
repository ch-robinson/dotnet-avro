#pragma warning disable SA1401 // allow public fields

namespace Chr.Avro.Fixtures
{
    using System.ComponentModel;

    [Description("Test")]
    public class DescriptionAnnotatedClass
    {
        public int UnannotatedField;

        [Description("Test")]
        public string DescriptionField;
    }
}
