using System;
using System.Collections.Generic;
using System.Text;

namespace Chr.Avro.Example.Models
{
    public class ExampleMappedClass
    {
        public DateTime DeliverByDate { get; set; }

        public int Id { get; set; }

        public List<ExampleItem> Items { get; set; }

        public ExampleMappedClass Class { get; set; }

        public int OptionalArgument { get; set; }

        public ExampleMappedClass()
        {

        }

        public ExampleMappedClass(int id, List<ExampleItem> items, DateTime expectedDate, ExampleMappedClass @class, int optionalArgument = 5)
        {
            DeliverByDate = expectedDate;
            Id = id;
            Items = items;
            Class = @class;
            OptionalArgument = optionalArgument;
        }
    }
}
