#pragma warning disable CA1069 // allow duplicate enum values

namespace Chr.Avro.Fixtures
{
    using System;
    using System.Runtime.Serialization;

    public enum DataContractNonAnnotatedEnum
    {
        None,

        [EnumMember]
        Default,

        [EnumMember(Value = "Different")]
        Custom = 2,

        [NonSerialized]
        Ignored,

        [EnumMember]
        [NonSerialized]
        Conflicting = 1,
    }
}
