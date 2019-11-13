using System;
using System.Collections.Generic;
using System.Text;

namespace Chr.Avro.Example.Models
{
    public class ExampleClass
    {
        public DateTime ExpectedDate { get; set; }

        public int Id { get; set; }

        public List<ExampleItem> Items { get; set; }
    }

    public class ExampleItem
    {
        public int Id { get; set; }

        public string Description { get; set; }
    }
}
