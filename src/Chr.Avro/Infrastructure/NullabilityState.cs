#if !NET6_0_OR_GREATER
namespace Chr.Avro.Infrastructure
{
    /// <summary>
    /// Describes nullability states.
    /// </summary>
    /// <remarks>
    /// This type is a stand-in for the <c>NullabilityState</c> enum available in .NET 6 and above. See
    /// <see href="https://github.com/dotnet/runtime/blob/v6.0.0/src/libraries/System.Private.CoreLib/src/System/Reflection/NullabilityInfo.cs">the .NET runtime source</see>
    /// (also MIT licensed) for the reference implementation.
    /// </remarks>
    internal enum NullabilityState
    {
        /// <summary>
        /// Nullability context not enabled (oblivious).
        /// </summary>
        Unknown,

        /// <summary>
        /// Non-nullable value or reference type.
        /// </summary>
        NotNull,

        /// <summary>
        /// Nullable value or reference type.
        /// </summary>
        Nullable,
    }
}
#endif
