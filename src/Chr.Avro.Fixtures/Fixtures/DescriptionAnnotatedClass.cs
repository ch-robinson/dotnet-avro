#pragma warning disable SA1401 // allow public fields

namespace Chr.Avro.Fixtures
{
    using System.ComponentModel;

    [Description("Class Description")]
    public class DescriptionAnnotatedClass
    {
        public int UnannotatedField;

        [Description("Field Description")]
        public string DescriptionField;

        [Description("Property Description")]
        public string DescriptionProperty;

        [Description("Property \"Description\" with double quotes")]
        public string DescriptionPropertyWithDoubleQuotes;
    }
}
