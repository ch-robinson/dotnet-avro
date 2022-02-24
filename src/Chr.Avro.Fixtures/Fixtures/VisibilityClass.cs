#pragma warning disable CA2211  // allow public static fields
#pragma warning disable CS0169  // allow unused private fields
#pragma warning disable CS0649  // allow unused internal fields
#pragma warning disable IDE0044 // allow private fields without readonly
#pragma warning disable IDE0051 // allow unused private members
#pragma warning disable SA1306  // allow private/protected member names to be Pascal case
#pragma warning disable SA1401  // allow public fields

namespace Chr.Avro.Fixtures
{
    public class VisibilityClass
    {
        public static int PublicStaticField;

        public int PublicField;

        internal static int InternalStaticField;

        internal int InternalField;

        protected static int ProtectedStaticField;

        protected int ProtectedField;

        private static int PrivateStaticField;

        private int PrivateField;

        public static int PublicStaticProperty { get; set; }

        public int PublicProperty { get; set; }

        internal static int InternalStaticProperty { get; set; }

        internal int InternalProperty { get; set; }

        protected static int ProtectedStaticProperty { get; set; }

        protected int ProtectedProperty { get; set; }

        private static int PrivateStaticProperty { get; set; }

        private int PrivateProperty { get; set; }
    }
}
