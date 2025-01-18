using System;
using System.Runtime.CompilerServices;

namespace Chr.Avro;

internal static class StringConverter
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ConvertToPascalCase(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        // Allocate the maximum possible size (input length)
        Span<char> result = stackalloc char[input.Length];
        var writeIndex = 0;
        var capitalizeNext = true;

        for (var i = 0; i < input.Length; i++)
        {
            var currentChar = input[i];

            if (currentChar == '_')
            {
                capitalizeNext = true;
                continue;
            }

            result[writeIndex++] = capitalizeNext
                ? char.ToUpperInvariant(currentChar)
                : char.ToLowerInvariant(currentChar);

            capitalizeNext = false;
        }

        return new string(result.Slice(0, writeIndex).ToArray());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ConvertToNamespaceCase(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        // Allocate the maximum possible size (input length)
        Span<char> result = stackalloc char[input.Length];
        var writeIndex = 0;
        var capitalizeNext = true;

        for (var i = 0; i < input.Length; i++)
        {
            var currentChar = input[i];

            if (currentChar == '.')
            {
                result[writeIndex++] = '.';
                capitalizeNext = true;
                continue;
            }

            if (currentChar == '_')
            {
                capitalizeNext = true;
                continue;
            }

            result[writeIndex++] = capitalizeNext
                ? char.ToUpperInvariant(currentChar)
                : char.ToLowerInvariant(currentChar);

            capitalizeNext = false;
        }

        return new string(result.Slice(0, writeIndex).ToArray());
    }
}
