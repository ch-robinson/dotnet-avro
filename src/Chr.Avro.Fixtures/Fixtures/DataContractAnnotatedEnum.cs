#pragma warning disable CA1069 // allow duplicate enum values

namespace Chr.Avro.Fixtures
{
    using System;
    using System.Runtime.Serialization;

    [DataContract(Name = "annotated", Namespace = "chr.fixtures")]
    public enum DataContractAnnotatedEnum
    {
        None,

        [EnumMember]
        Default,

        [EnumMember(Value = "Different")]
        Custom = 2,

        [IgnoreDataMember]
        Ignored,

        [NonSerialized]
        NonSerialized,

        [EnumMember]
        [NonSerialized]
        Conflicting = 1,
    }
}
