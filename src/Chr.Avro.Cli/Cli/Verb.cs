namespace Chr.Avro.Cli
{
    using System;
    using System.Threading.Tasks;

    public abstract class Verb
    {
        public async Task<int> Execute()
        {
            try
            {
                await Run();
            }
            catch (ProgramException e)
            {
                if (e.Message is var message && !string.IsNullOrEmpty(message))
                {
                    Console.Error.WriteLine(message);
                }

                return e.Code;
            }

            return 0;
        }

        protected abstract Task Run();
    }
}
