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
        }

        public MyContext(CqlConnection connection, bool ownsConnection)
            : base(connection, ownsConnection)
        {
        }

        public CqlTable<MyValue> Values { get; set; }
    }
}