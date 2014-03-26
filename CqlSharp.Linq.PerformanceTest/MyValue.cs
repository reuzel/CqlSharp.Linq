using CqlSharp.Serialization;

namespace CqlSharp.Linq.PerformanceTest
{
    /// <summary>
    ///   class representing the values in a table
    /// </summary>
    public class MyValue
    {
        [CqlKey]
        public int Id { get; set; }

        public string Value { get; set; }
    }
}