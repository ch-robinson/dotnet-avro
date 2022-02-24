#pragma warning disable CS8618

namespace Chr.Avro.Fixtures
{
    using System;
    using System.Collections.Generic;

    public class NullableMemberClass
    {
        public Guid ObliviousGuidProperty { get; set; }

        public Guid? ObliviousNullableGuidProperty { get; set; }

        public string ObliviousStringProperty { get; set; }

        public string[] ObliviousArrayOfStringsProperty { get; set; }

        public List<string> ObliviousListOfStringsProperty { get; set; }

        public Dictionary<string, object> ObliviousDictionaryOfObjectsProperty { get; set; }

#nullable enable
        public Guid GuidProperty { get; set; }

        public Guid? NullableGuidProperty { get; set; }

        public string StringProperty { get; set; }

        public string? NullableStringProperty { get; set; }

        public string[] ArrayOfStringsProperty { get; set; }

        public string?[] ArrayOfNullableStringsProperty { get; set; }

        public string[]? NullableArrayOfStringsProperty { get; set; }

        public string?[]? NullableArrayOfNullableStringsProperty { get; set; }

        public List<string> ListOfStringsProperty { get; set; }

        public List<string?> ListOfNullableStringsProperty { get; set; }

        public List<string>? NullableListOfStringsProperty { get; set; }

        public List<string?>? NullableListOfNullableStringsProperty { get; set; }

        public Dictionary<string, object> DictionaryOfObjectsProperty { get; set; }

        public Dictionary<string, object?> DictionaryOfNullableObjectsProperty { get; set; }

        public Dictionary<string, object>? NullableDictionaryOfObjectsProperty { get; set; }

        public Dictionary<string, object?>? NullableDictionaryOfNullableObjectsProperty { get; set; }
#nullable disable
    }
}
