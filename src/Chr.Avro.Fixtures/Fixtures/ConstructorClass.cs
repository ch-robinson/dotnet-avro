namespace Chr.Avro.Fixtures
{
    public class ConstructorClass
    {
        public ConstructorClass(int fieldA, string fieldB = null)
        {
            FieldA = fieldA;
            FieldB = fieldB;
        }

        public int FieldA { get; }

        public string FieldB { get; }
    }
}
