namespace Chr.Avro.UnionTypeExample.Models
{
    using System;

    public class OrderEventRecord
    {
        public IOrderEvent Event { get; set; }

        public DateTime Timestamp { get; set; }
    }
}
