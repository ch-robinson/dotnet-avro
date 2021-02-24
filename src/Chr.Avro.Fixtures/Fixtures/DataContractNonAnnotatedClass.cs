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

        [NonSerialized]
        public int IgnoredField;

        [DataMember]
        [NonSerialized]
        public int ConflictingField;

        public int UnannotatedProperty { get; set; }

        [DataMember]
        public int AnnotatedDefaultProperty { get; set; }

        [DataMember(Name = "DifferentProperty", Order = 1)]
        public int AnnotatedCustomProperty { get; set; }
    }
}
