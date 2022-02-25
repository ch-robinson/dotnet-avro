namespace Chr.Avro.UnionTypeExample.Models
{
    using System.Collections.Generic;

    public class OrderCreationEvent : IOrderEvent
    {
        public IList<OrderLineItem> LineItems { get; set; }
    }
}
