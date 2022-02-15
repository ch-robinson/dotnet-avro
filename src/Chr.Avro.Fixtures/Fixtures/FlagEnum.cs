namespace Chr.Avro.Fixtures
{
    using System;

    [Flags]
    public enum FlagEnum
    {
        None = 0,
        First = 1,
        Second = 2,
        Third = 4,
        Fourth = 8,
    }
}
