namespace Chr.Avro.NodaTimeExample.Models
{
    using System;
    using NodaTime;

    public class Player
    {
        public Guid Id { get; set; }

        public string Nickname { get; set; }

        public Instant LastLogin { get; set; }
    }
}
