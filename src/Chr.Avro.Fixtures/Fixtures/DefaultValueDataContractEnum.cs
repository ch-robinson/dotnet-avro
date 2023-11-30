using System.ComponentModel;
using System.Runtime.Serialization;

namespace Chr.Avro.Fixtures
{
    [DefaultValue(Third)]
    [DataContract]
    public enum DefaultValueDataContractEnum
    {
        [EnumMember]
        First,
        [EnumMember]
        Second,
        [EnumMember(Value = "AliasedDefaultValue")]
        Third,
        [EnumMember]
        Fourth,
    }
}
