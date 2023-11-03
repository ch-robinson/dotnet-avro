#pragma warning disable SA1401 // allow public fields

namespace Chr.Avro.Fixtures
{
    using System;
    using System.Runtime.Serialization;

    public class DataContractNonAnnotatedClass
    {
        public int UnannotatedField;

        [DataMember]
        public int AnnotatedDefaultField;

        [DataMember(Name = "DifferentField", Order = 5)]
        public int AnnotatedCustomField;

        [IgnoreDataMember]
        public int IgnoredField;

        [NonSerialized]
        public int NonSerializedField;

        [DataMember]
        [NonSerialized]
        public int ConflictingField;

        public int UnannotatedProperty { get; set; }

        [DataMember]
        public int AnnotatedDefaultProperty { get; set; }

        [DataMember(Name = "DifferentProperty", Order = 1)]
        public int AnnotatedCustomProperty { get; set; }

        [IgnoreDataMember]
        public int IgnoredProperty { get; set; }
    }
}
