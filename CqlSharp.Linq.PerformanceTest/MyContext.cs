namespace CqlSharp.Linq.PerformanceTest
{
    /// <summary>
    ///   The context used for testing
    /// </summary>
    public class MyContext : CqlContext
    {
        public MyContext()
        {
        }

        public MyContext(string connectionString)
            : base(connectionString)
        {
            Database.Keyspace = "linqperftest";
        }

        public MyContext(CqlConnection connection, bool ownsConnection)
            : base(connection, ownsConnection)
        {
            Database.Keyspace = "linqperftest";
        }

        public CqlTable<MyValue> Values { get; set; }
    }
}