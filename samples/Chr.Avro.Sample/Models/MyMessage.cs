using System;
using System.Collections.Generic;

namespace Chr.Avro.Sample.Models
{
    /// <summary>
    /// Class MyMessage.
    /// </summary>
    public class MyMessage
    {
        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>The name.</value>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the payload.
        /// </summary>
        /// <value>The payload.</value>
        /// TODO Edit XML Comment Template for Payload
        public Dictionary<string, IDataObj> Payload { get; set; }

        public DateTime DateTime { get; set; }
    }
}
