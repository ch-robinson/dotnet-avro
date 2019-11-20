using System;
using System.Runtime.Serialization;

#pragma warning disable IDE0044 // private fields
#pragma warning disable CS0169, CS0649, IDE0051 // unused fields

namespace Chr.Avro.Tests
{
    public interface IEmptyInterface { }

    public class EmptyClass { }

    public struct EmptyStruct { }

    public interface IVisibilityInterface
    {
        int PublicProperty { get; }
    }

    public class VisibilityClass
    {
        public int PublicField;

        internal int InternalField;

        protected int ProtectedField;

        private int PrivateField;

        public int PublicProperty { get; set; }

        internal int InternalProperty { get; set; }

        protected int ProtectedProperty { get; set; }

        private int PrivateProperty { get; set; }

        public static int PublicStaticField;

        internal static int InternalStaticField;

        protected static int ProtectedStaticField;

        private static int PrivateStaticField;

        public static int PublicStaticProperty { get; set; }

        internal static int InternalStaticProperty { get; set; }

        protected static int ProtectedStaticProperty { get; set; }

        private static int PrivateStaticProperty { get; set; }
    }

    public class CircularClass
    {
        public CircularClass Child { get; set; }
    }

    public class CircularClassA
    {
        public CircularClassB B { get; set; }
    }

    public class CircularClassB
    {
        public CircularClassA A { get; set; }
    }

    public class ConstructorClass
    {
        public int FieldA { get; set; }

        public string FieldB { get; set; }

        public ConstructorClass(int fieldA, string fieldB = null)
        {
            FieldA = fieldA;
            FieldB = fieldB;
        }
    }

    [DataContract(Name = "annotated", Namespace = "chr.tests")]
    public class DataContractAnnotatedClass
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

    public class NullablePropertyClass
    {
        public Guid Id { get; set; }

        public DateTime Created { get; set; }

        public DateTime? Updated { get; set; }

        public DateTime? Deleted { get; set; }
    }
}
