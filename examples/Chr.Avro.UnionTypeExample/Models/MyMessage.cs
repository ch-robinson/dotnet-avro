namespace Chr.Avro.UnionTypeExample.Models
{
    using System;
    using System.Collections.Generic;

    public class MyMessage
    {
        public string Name { get; set; }

        public Dictionary<string, IDataObj> Payload { get; set; }

        public DateTime DateTime { get; set; }
    }
}
