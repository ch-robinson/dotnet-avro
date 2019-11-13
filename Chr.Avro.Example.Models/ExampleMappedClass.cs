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

        public ExampleMappedClass()
        {

        }

        public ExampleMappedClass(DateTime expectedDate, int id, List<ExampleItem> items)
        {
            DeliverByDate = expectedDate;
            Id = id;
            Items = items;
        }
    }
}
