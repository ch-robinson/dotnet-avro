namespace Chr.Avro.UnionTypeExample.Models
{
    public class OrderLineItemModificationEvent : IOrderEvent
    {
        public int Index { get; set; }

        public OrderLineItem LineItem { get; set; }
    }
}
