using CqlSharp.Protocol;
using System;
using System.Diagnostics;
using System.Linq;

namespace CqlSharp.Linq.PerformanceTest
{
    class Program
    {
        private const string ConnectionString =
           "server=localhost;throttle=256;MaxConnectionIdleTime=3600;loggerfactory=debug;loglevel=Verbose;username=cassandra;password=cassandra";


        static void Main(string[] args)
        {
            var st = new Stopwatch();
            st.Start();

            Console.WriteLine("{0}ms => Setup database", st.ElapsedMilliseconds);

            SetupDatabase();

            Console.WriteLine("{0}ms => Start query", st.ElapsedMilliseconds);

            for (int i = 0; i < 50;i++)
                QueryMany();

            Console.WriteLine("{0}ms => End query", st.ElapsedMilliseconds);
        }

        static void QueryMany()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var allRecords = context.Values.WithPageSize(1000).ToList();
                if (allRecords.Count == 0)
                    throw new Exception("no results!");
            }
        }

        static void SetupDatabase()
        {

            const string createKsCql =
                @"CREATE KEYSPACE linqperftest WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";

            const string createTableCql =
                @"create table linqperftest.myvalue (id int primary key, value text, ignored text);";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                try
                {
                    var createKs = new CqlCommand(connection, createKsCql);
                    createKs.ExecuteNonQuery();
                }
                catch (AlreadyExistsException)
                {
                }

                try
                {
                    var createTable = new CqlCommand(connection, createTableCql);
                    createTable.ExecuteNonQuery();

                    using (var transaction = connection.BeginTransaction())
                    {
                        transaction.BatchType = CqlBatchType.Unlogged;

                        var insert = new CqlCommand(connection, "insert into linqperftest.myvalue (id,value) values(?,?)");
                        insert.Transaction = transaction;
                        insert.Prepare();

                        for (int i = 0; i < 10000; i++)
                        {
                            insert.Parameters[0].Value = i;
                            insert.Parameters[1].Value = "Hallo " + i;
                            insert.ExecuteNonQuery();
                        }

                        transaction.Commit();
                    }

                }
                catch (AlreadyExistsException)
                {
                }
            }
        }
    }
}
