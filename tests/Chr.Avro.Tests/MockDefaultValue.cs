namespace Chr.Avro.Tests
{
    using Chr.Avro.Abstract;

    public class MockDefaultValue : DefaultValue
    {
        public MockDefaultValue(Schema schema)
            : base(schema)
        {
        }

        public override T ToObject<T>()
        {
            throw new System.NotImplementedException();
        }
    }
}
