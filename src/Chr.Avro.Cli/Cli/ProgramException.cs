namespace Chr.Avro.Cli
{
    using System;

    [Serializable]
    public sealed class ProgramException : Exception
    {
        public ProgramException(int code = 1, string message = null, Exception inner = null)
            : base(message, inner)
        {
            Code = code;
        }

        public int Code { get; }
    }
}
