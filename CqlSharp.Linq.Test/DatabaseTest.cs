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

using System.Threading.Tasks;
using CqlSharp.Linq.Mutations;
using CqlSharp.Protocol;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
using System.Linq;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class DatabaseTest
    {
        private const string ConnectionString =
            "server=localhost;throttle=256;MaxConnectionIdleTime=3600;loggerfactory=debug;loglevel=Verbose;username=cassandra;password=cassandra;database=linqtest";

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

                    using (var transaction = connection.BeginTransaction())
                    {
                        transaction.BatchType = CqlBatchType.Unlogged;

                        var insert = new CqlCommand(connection, "insert into linqtest.myvalue (id,value) values(?,?)");
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
                    //ignore
                }
            }

            CqlConnection.Shutdown(createConnection);
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            const string dropCql = @"drop keyspace linqtest;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                try
                {
                    var drop = new CqlCommand(connection, dropCql);
                    drop.ExecuteNonQuery();
                }
                catch (InvalidException)
                {
                    //ignore
                }
            }

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
        [ExpectedException(typeof(InvalidException))]
        public void WhereUsingWrongKey()
        {
            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Where(v => v.Value == "Hallo 1").Select(v => v.Id).First();
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

        [TestMethod]
        public void FindAndUpdate()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);
                var value = context.Values.Find(100);
                value.Value = "Hallo daar!";
                context.SaveChanges();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(100);
                Assert.IsNotNull(value);
                Assert.AreEqual("Hallo daar!", value.Value);
            }
        }

        [TestMethod]
        public void UpdateTwiceInSingleTransaction()
        {
            using (var context = new MyContext(ConnectionString))
            using (var transaction = context.Database.BeginTransaction())
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                var value = context.Values.Find(200);
                value.Value = "Hallo daar!";
                context.SaveChanges();

                value.Value = "Oops...";
                context.SaveChanges();

                transaction.Commit();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(200);
                Assert.IsNotNull(value);
                Assert.AreEqual("Oops...", value.Value);
            }
        }

        [TestMethod]
        public void UpdateTwiceInSingleTransactionAndRollback()
        {
            using (var context = new MyContext(ConnectionString))
            using (var transaction = context.Database.BeginTransaction())
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                var value = context.Values.Find(300);
                value.Value = "Hallo daar!";
                context.SaveChanges();

                value.Value = "Oops...";
                context.SaveChanges();

                transaction.Rollback();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(300);
                Assert.IsNotNull(value);
                Assert.AreEqual("Hallo 300", value.Value);
            }
        }

        [TestMethod]
        public void UpdateTwiceInTwoTransactions()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                var value = context.Values.Find(400);

                using (var transaction1 = context.Database.BeginTransaction())
                {
                    value.Value = "Hallo daar!";
                    context.SaveChanges();
                    transaction1.Commit();
                }

                using (var transaction2 = context.Database.BeginTransaction())
                {
                    transaction2.BatchType = CqlBatchType.Unlogged;

                    value.Value = "Nog een keer";
                    context.SaveChanges();
                    transaction2.Commit();
                }
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(400);
                Assert.IsNotNull(value);
                Assert.AreEqual("Nog een keer", value.Value);
            }
        }

        [TestMethod]
        public void UpdateInExternalTransaction()
        {
            using (var connection = new CqlConnection(ConnectionString))
            using (var transaction = connection.BeginTransaction())
            using (var context = new MyContext(connection, false))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);

                context.Database.UseTransaction(transaction);

                var value = context.Values.Find(500);
                value.Value = "Hallo daar!";
                context.SaveChanges(false);

                var command = new CqlCommand(connection, "update myvalue set value='adjusted' where id=500");
                command.Transaction = transaction;
                command.ExecuteNonQuery();

                transaction.Commit();

                //accept all changes only after commit
                context.AcceptAllChanges();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(500);
                Assert.IsNotNull(value);
                Assert.AreEqual("adjusted", value.Value);
            }
        }

        [TestMethod]
        public void SelectAndUpdate()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);
                var query = context.Values.Where(r => new[] { 201, 202, 203, 204 }.Contains(r.Id)).ToList();
                query[1].Value = "Zo gaan we weer verder";
                context.SaveChanges();
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(202);
                Assert.IsNotNull(value);
                Assert.AreEqual("Zo gaan we weer verder", value.Value);
            }
        }

        [TestMethod]
        public async Task SelectAndDelete()
        {
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql => Debug.WriteLine("EXECUTE QUERY: " + cql);
                var query = context.Values.Where(r => new[] { 701, 702, 703, 704 }.Contains(r.Id)).ToList();

                TrackedEntity<MyValue> entry;
                Assert.IsTrue(context.ChangeTracker.TryGetEntry(query[1], out entry));

                Assert.AreEqual(EntityState.Unchanged, entry.State);

                context.Values.Delete(query[1]);

                Assert.AreEqual(EntityState.Deleted, entry.State);

                await context.SaveChangesAsync();

                Assert.AreEqual(EntityState.Detached, entry.State);

            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(702);
                Assert.IsNull(value, "Value was not deleted");
            }
        }

        [TestMethod]
        public void AddNewEntity()
        {
            int count = 0;
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql =>
                {
                    count++;
                    Debug.WriteLine("EXECUTE QUERY: " + cql);
                };

                var newValue = new MyValue { Id = 20000, Value = "Hallo 20000" };
                bool added = context.Values.Add(newValue);
                Assert.IsTrue(added);
                context.SaveChanges();

                //try save again (should do nothing)
                context.SaveChanges();
                Assert.AreEqual(1, count, "Save again introduces new query!");

                //try find (should do nothing)
                var entity = context.Values.Find(20000);
                Assert.AreSame(newValue, entity);
                Assert.AreEqual(1, count, "Find introduces new query!");
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(20000);
                Assert.IsNotNull(value);
                Assert.AreEqual("Hallo 20000", value.Value);
            }
        }

        [TestMethod]
        public void AddAndChangeNewEntity()
        {
            int count = 0;
            using (var context = new MyContext(ConnectionString))
            {
                context.Database.Log = cql =>
                {
                    count++;
                    Debug.WriteLine("EXECUTE QUERY: " + cql);
                };

                var newValue = new MyValue { Id = 30000, Value = "Hallo 30000" };
                bool added = context.Values.Add(newValue);
                Assert.IsTrue(added);


                ITrackedEntity entry;
                Assert.IsTrue(context.ChangeTracker.TryGetEntry(newValue, out entry));
                Assert.AreEqual(EntityState.Added, entry.State);

                context.SaveChanges();

                Assert.AreEqual(EntityState.Unchanged, entry.State);

                newValue.Value = "Hallo weer!";
                context.SaveChanges();
                Assert.AreEqual(2, count, "Where is my query?");
            }

            using (var context = new MyContext(ConnectionString))
            {
                var value = context.Values.Find(30000);
                Assert.IsNotNull(value);
                Assert.AreEqual("Hallo weer!", value.Value);
            }
        }
    }
}