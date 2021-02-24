namespace Chr.Avro.Fixtures
{
    using System;

    public class NullablePropertyClass
    {
        public Guid Id { get; set; }

        public DateTime Created { get; set; }

        public DateTime? Updated { get; set; }

        public DateTime? Deleted { get; set; }
    }
}
