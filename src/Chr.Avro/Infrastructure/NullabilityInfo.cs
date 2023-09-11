#if !NET6_0_OR_GREATER
namespace Chr.Avro.Infrastructure
{
    using System;

    /// <summary>
    /// Represents nullability information.
    /// </summary>
    /// <remarks>
    /// This type is a stand-in for the <c>NullabilityInfo</c> class available in .NET 6 and above. See
    /// <see href="https://github.com/dotnet/runtime/blob/v6.0.0/src/libraries/System.Private.CoreLib/src/System/Reflection/NullabilityInfo.cs">the .NET runtime source</see>
    /// (also MIT licensed) for the reference implementation.
    /// </remarks>
    internal sealed class NullabilityInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NullabilityInfo" /> class.
        /// </summary>
        /// <param name="type">
        /// The type of the member or generic parameter to which this instance belongs.
        /// </param>
        /// <param name="readState">
        /// The nullability read state of the member.
        /// </param>
        /// <param name="writeState">
        /// The nullability write state of the member.
        /// </param>
        /// <param name="elementType">
        /// The nullability information for the element type of the array.
        /// </param>
        /// <param name="typeArguments">
        /// The nullability information for each type parameter.
        /// </param>
        internal NullabilityInfo(
            Type type,
            NullabilityState readState,
            NullabilityState writeState,
            NullabilityInfo? elementType,
            NullabilityInfo[] typeArguments)
        {
            Type = type;
            ReadState = readState;
            WriteState = writeState;
            ElementType = elementType;
            GenericTypeArguments = typeArguments;
        }

        /// <summary>
        /// Gets the type of the member or generic parameter to which this instance belongs.
        /// </summary>
        public Type Type { get; }

        /// <summary>
        /// Gets or sets the nullability read state of the member.
        /// </summary>
        public NullabilityState ReadState { get; internal set; }

        /// <summary>
        /// Gets or sets the nullability write state of the member.
        /// </summary>
        public NullabilityState WriteState { get; internal set; }

        /// <summary>
        /// Gets the nullability information for the element type of the array.
        /// </summary>
        public NullabilityInfo? ElementType { get; }

        /// <summary>
        /// Gets the nullability information for each type parameter.
        /// </summary>
        public NullabilityInfo[] GenericTypeArguments { get; }
    }
}
#endif
