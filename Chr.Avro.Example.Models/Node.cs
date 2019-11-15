using System;
using System.Collections.Generic;
using System.Text;

namespace Chr.Avro.Example.Models
{
    public class Node
    {
        public Node InnerClass { get; set; }

        public Node(Node innerClass)
        {
            InnerClass = innerClass;
        }
    }
}
