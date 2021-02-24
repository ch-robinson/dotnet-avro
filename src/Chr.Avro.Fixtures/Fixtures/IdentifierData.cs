namespace Chr.Avro.Fixtures
{
    using Xunit;

    public static class IdentifierData
    {
        public static readonly TheoryData<string> InvalidIdentifiers = new ()
        {
            { string.Empty },
            { "1symbol" },
            { "space space" },
            { "kebab-case" },
            { "namespace.symbol" },
        };

        public static readonly TheoryData<string, string, string> NamespacedIdentifiersWithInvalidNames = new ()
        {
            { string.Empty, null, string.Empty },
            { "space space", null, "space space" },
            { "kebab-case", null, "kebab-case" },
            { "1nvalid", null, "1nvalid" },
            { "inv?l!d", null, "inv?l!d" },
            { "name.", "name", string.Empty },
            { "name.name.", "name.name", string.Empty },
        };

        public static readonly TheoryData<string, string, string> NamespacedIdentifiersWithInvalidNamespaces = new ()
        {
            { ".name", string.Empty, "name" },
            { "..name", ".", "name" },
            { "space space.name", "space space", "name" },
            { "2x.4y.6z.name", "2x.4y.6z", string.Empty },
        };

        public static readonly TheoryData<string> ValidIdentifiers = new ()
        {
            { "lowercase" },
            { "UPPERCASE" },
            { "snake_case" },
            { "_" },
            { "symbol1" },
        };

        public static readonly TheoryData<string, string, string> ValidNamespacedIdentifiers = new ()
        {
            { "lowercase", null, "lowercase" },
            { "PascalCase", null, "PascalCase" },
            { "snake_case", null, "snake_case" },
            { "_", null, "_" },
            { "a8", null, "a8" },
            { "_6", null, "_6" },
            { "namespaced.name", "namespaced", "name" },
            { "deeper.namespaced.name", "deeper.namespaced", "name" },
            { "_._", "_", "_" },
            { "x2.y4.z6.a8", "x2.y4.z6", "a8" },
        };
    }
}
