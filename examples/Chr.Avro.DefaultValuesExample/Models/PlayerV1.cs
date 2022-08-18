namespace Chr.Avro.DefaultValuesExample.Models
{
    using System;
    using System.ComponentModel;

    public class PlayerV1
    {
        public Guid Id { get; set; }

        public string Nickname { get; set; }

        [DefaultValue(100)]
        public int Health { get; set; }
    }
}
