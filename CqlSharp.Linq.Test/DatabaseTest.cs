// CqlSharp.Linq - CqlSharp.Linq.Test
// Copyright (c) 2014 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using CqlSharp.Protocol;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class DatabaseTest
    {
        private const string ConnectionString =
            "server=localhost;throttle=256;MaxConnectionIdleTime=3600;loggerfactory=debug;loglevel=query;username=cassandra;password=cassandra;database=linqtest";

        [ClassInitialize]
        public static void Init(TestContext context)
        {
            const string createConnection = "Server=localhost;username=cassandra;password=cassandra";

            const string createKsCql =
                @"CREATE KEYSPACE LinqTest WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";
            const string createTableCql =
                @"create table linqtest.myvalue (id int primary key, value text, ignored text);";


            using (var connection = new CqlConnection(createConnection))
            {
                connection.SetConnectionTimeout(0);
                connection.Open();

                try
                {
                    var createKs = new CqlCommand(connection, createKsCql);
                    createKs.ExecuteNonQuery();

                    var createTable = new CqlCommand(connection, createTableCql);
                    createTable.ExecuteNonQuery();

                    var insert = new CqlCommand(connection, "insert into linqtest.myvalue (id,value) values(?,?)");
                    insert.Prepare();

                    for (int i = 0; i < 10000; i++)
                    {
                        insert.Parameters[0].Value = i;
                        insert.Parameters[1].Value = "Hallo " + i;
                        insert.ExecuteNonQuery();
                    }
                }
                catch (AlreadyExistsException)
                {
                    //ignore
                }
            }

            CqlConnection.Shutdown(createConnection);
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            //const string dropCql = @"drop keyspace linqtest;";

            //using (var connection = new CqlConnection(ConnectionString))
            //{
            //    connection.Open();

            //    try
            //    {
            //        var drop = new CqlCommand(connection, dropCql);
            //        drop.ExecuteNonQuery();
            //    }
            //    catch (InvalidException)
            //    {
            //        //ignore
            //    }
            //}

            CqlConnection.ShutdownAll();
        }

        [TestMethod]
        public void WhereThenSelect()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Where(v => v.Id == 1).Select(v => v.Value).First();
                Assert.AreEqual("Hallo 1", value);
            }
        }

        [TestMethod]
        public void WhereContains()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var values = context.Values.Where(r => new[] { 1, 2, 3, 4 }.Contains(r.Id)).ToList();

                Assert.AreEqual(4, values.Count, "Unexpected number of results");
                for (int i = 1; i <= 4; i++)
                {
                    Assert.IsTrue(values.Any(v => v.Id == i), "Missing value " + i);
                }
            }
        }

        [TestMethod]
        public void EmumerateMultiple()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var query = context.Values.Where(r => new[] { 1, 2, 3, 4 }.Contains(r.Id));

                Assert.AreEqual(4, query.Count(), "Unexpected number of results");
                var results = query.ToList();
                for (int i = 1; i <= 4; i++)
                {
                    Assert.IsTrue(results.Any(v => v.Id == i), "Missing value " + i);
                }
            }
        }
    }
}