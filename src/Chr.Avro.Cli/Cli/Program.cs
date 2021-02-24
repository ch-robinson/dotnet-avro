namespace Chr.Avro.Cli
{
    using System;
    using System.Threading.Tasks;
    using CommandLine;

    public static class Program
    {
        private static readonly Parser Parser = new (settings =>
        {
            settings.CaseInsensitiveEnumValues = true;
            settings.HelpWriter = Console.Error;
        });

        public static Task Main(string[] args)
        {
            return Parser
                .ParseArguments<CreateSchemaVerb, GenerateCodeVerb, GetSchemaVerb, TestSchemaVerb>(args)
                .MapResult(
                    (CreateSchemaVerb create) => create.Execute(),
                    (GenerateCodeVerb generate) => generate.Execute(),
                    (GetSchemaVerb get) => get.Execute(),
                    (TestSchemaVerb verify) => verify.Execute(),
                    errors => Task.FromResult(1));
        }
    }
}
