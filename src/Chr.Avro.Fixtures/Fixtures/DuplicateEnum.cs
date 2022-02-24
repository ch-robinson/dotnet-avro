#pragma warning disable CA1069 // allow duplicate enum values

namespace Chr.Avro.Fixtures
{
    public enum DuplicateEnum
    {
        B = 0,
        C = 0,
        A = 0,
    }
}
