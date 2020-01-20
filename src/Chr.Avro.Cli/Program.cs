using CommandLine;
using System;
using System.Threading.Tasks;

namespace Chr.Avro.Cli
{
    public static class Program
    {
        private static readonly Parser _parser = new Parser(settings =>
        {
            settings.CaseInsensitiveEnumValues = true;
            settings.HelpWriter = Console.Error;
        });

        public static Task Main(string[] args)
        {
            return _parser
                .ParseArguments<CreateSchemaVerb, GenerateCodeVerb, GetSchemaVerb, TestSchemaVerb>(args)
                .MapResult(
                    (CreateSchemaVerb create) => create.Execute(),
                    (GenerateCodeVerb generate) => generate.Execute(),
                    (GetSchemaVerb get) => get.Execute(),
                    (TestSchemaVerb verify) => verify.Execute(),
                    errors => Task.FromResult(1)
                );
        }
    }

    [Serializable]
    public sealed class ProgramException : Exception
    {
        public int Code { get; private set; }

        public ProgramException(int code = 1, string message = null, Exception inner = null) : base(message, inner)
        {
            Code = code;
        }
    }
}
