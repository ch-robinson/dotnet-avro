namespace Chr.Avro.Fixtures
{
    using System.ComponentModel;
    using System.Runtime.Serialization;

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
