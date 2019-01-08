using System;

namespace Chr.Avro.Tests
{
    internal enum DuplicateEnum
    {
        B = 0,
        C = 0,
        A = 0
    }

    internal enum EmptyEnum { }

    internal enum ExplicitEnum
    {
        None = 0,
        First = -1,
        Second = 4,
        Third = -9,
        Fourth = 16,
    }

    [Flags]
    internal enum FlagEnum
    {
        None = 0,
        First = 1,
        Second = 2,
        Third = 4,
        Fourth = 8,
    }

    internal enum ImplicitEnum
    {
        None,
        First,
        Second,
        Third,
        Fourth,
    }
}
