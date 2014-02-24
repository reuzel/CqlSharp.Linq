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

using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class LinqTest
    {
        [TestMethod]
        public void WhereThenSelect()
        {
            var filter = "hallo";

            TestUtils.QueryFunc query =
                context => context.Values.Where(p => p.Value == filter + " daar").Select(r => r.Id).ToList();

            TestUtils.ExecuteQuery(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"value\"='hallo daar';");
        }

        [TestMethod]
        public void SelectThenWhere()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(r => r.Id).Where(id => id == 4).ToList();

            TestUtils.ExecuteQuery(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"id\"=4;");
        }

        [TestMethod]
        public void NoWhereOrSelect()
        {
            TestUtils.QueryFunc query = context => context.GetTable<AnnotatedTable>().ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"linqtest.myvalue\";");
        }

        [TestMethod]
        public void SelectAll()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(row => row).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\";");
        }

        [TestMethod]
        public void SelectIntoNewObject()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\";");
        }

        [TestMethod]
        public void WhereIdInArray()
        {
            TestUtils.QueryFunc query = context => context.Values.Where(r => new[] {1, 2, 3, 4}.Contains(r.Id)).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\" IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInList()
        {
            TestUtils.QueryFunc query = context => context.Values.Where(r => new List<int> {1, 2, 3, 4}.Contains(r.Id)).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\" IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInSet()
        {
            TestUtils.QueryFunc query =
                context => context.Values.Where(r => new HashSet<int> {1, 2, 3, 4}.Contains(r.Id)).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\" IN (1,2,3,4);");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException),
            "Type System.Collections.Generic.KeyValuePair`2[System.Int32,System.String] can not be converted to a valid CQL value"
            )]
        public void WhereKvpInDictionary()
        {
            TestUtils.QueryFunc query =
                context =>
                context.Values.Where(
                    r =>
                    new Dictionary<int, string> {{1, "a"}, {2, "b"}, {3, "c"}}.Contains(
                        new KeyValuePair<int, string>(r.Id, "a"))).ToList();
            TestUtils.ExecuteQuery(query, "No valid query");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException), "Type System.Char can\"t be converted to a CQL value")]
        public void WhereIdInNotSupportedListType()
        {
            TestUtils.QueryFunc query =
                context => context.Values.Where(r => new List<char> {'a', 'b', 'c'}.Contains((char) r.Id)).ToList();
            TestUtils.ExecuteQuery(query, "No valid query");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhere()
        {
            TestUtils.QueryFunc query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).Where(at => at.Id2 == 4).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=4;");
        }

        [TestMethod]
        public void SelectThenSelect()
        {
            TestUtils.QueryFunc query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id + 2, Value2 = r.Value}).Select(r2 => new {Id3 = r2.Id2}).
                    ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\" FROM \"myvalue\";");
        }

        [TestMethod]
        public void OnlyWhere()
        {
            TestUtils.QueryFunc query = context => context.Values.Where(r => r.Id == 2).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=2;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException), "CQL does not support the Add operator")]
        public void UnParsableWhereQuery()
        {
            TestUtils.QueryFunc query = context => context.Values.Where(r => r.Id + 2 == 4).ToList();
            TestUtils.ExecuteQuery(query, "no valid query");
        }

        [TestMethod]
        //[ExpectedException(typeof(CqlLinqException), "CQL does not support the Add operator")]
        public void WhereFromLinqToObjects()
        {
            var range = Enumerable.Range(1, 5);
            var selection = from r in range where r > 3 select r;

            TestUtils.QueryFunc query = context => context.Values.Where(r => selection.Contains(r.Id)).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\" IN (4,5);");
        }

        [TestMethod]
        public void OnlyFirst()
        {
            TestUtils.QueryFunc query = context => context.Values.First();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void FirstWithPredicate()
        {
            TestUtils.QueryFunc query = context => context.Values.First(v => v.Id == 2);
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenFirst()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(v => new {Id2 = v.Id}).First();
            TestUtils.ExecuteQuery(query, "SELECT \"id\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenWhereThenFirst()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(v => new {Id2 = v.Id}).Where(v2 => v2.Id2 == 2).First();
            TestUtils.ExecuteQuery(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenFirstWithPredicate()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(v => new {Id2 = v.Id}).First(v2 => v2.Id2 == 2);
            TestUtils.ExecuteQuery(query, "SELECT \"id\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 1;");
        }

        [TestMethod]
        public void OnlyFirstOrDefault()
        {
            TestUtils.QueryFunc query = context => context.Values.FirstOrDefault();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void FirstOrDefaultWithPredicate()
        {
            TestUtils.QueryFunc query = context => context.Values.FirstOrDefault(v => v.Id == 2);
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 1;");
        }

        [TestMethod]
        public void CountWithPredicate()
        {
            TestUtils.QueryFunc query = context => context.Values.Count(v => v.Id == 2);
            TestUtils.ExecuteQuery(query, "SELECT COUNT(*) FROM \"myvalue\" WHERE \"id\"=2;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeBeforeWhere()
        {
            //Wrong: logically first three items of values  table are taken, then where is performed on those three values, but Cql does not support sub-queries so this will not provide expected results
            TestUtils.QueryFunc query = context => context.Values.Take(3).Where(v => v.Id == 2).ToList();
            TestUtils.ExecuteQuery(query, "invalid query");
        }

        [TestMethod]
        public void WhereThenTake()
        {
            TestUtils.QueryFunc query = context => context.Values.Where(v => v.Id == 2).Take(3).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=2 LIMIT 3;");
        }

        [TestMethod]
        public void LargeTakeThenSmallTake()
        {
            TestUtils.QueryFunc query = context => context.Values.Take(3).Take(1).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void SmallTakeThenLargeTake()
        {
            TestUtils.QueryFunc query = context => context.Values.Take(1).Take(3).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" LIMIT 1;");
        }

        [TestMethod]
        public void TakeThenCount()
        {
            TestUtils.QueryFunc query = context => context.Values.Take(100).Count();
            TestUtils.ExecuteQuery(query, "SELECT COUNT(*) FROM \"myvalue\" LIMIT 100;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeThenCountWithCondition()
        {
            TestUtils.QueryFunc query = context => context.Values.Take(100).Count(v => v.Id > 100);
            TestUtils.ExecuteQuery(query, "invalid query");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhereThenTake()
        {
            TestUtils.QueryFunc query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).Where(at => at.Id2 == 4).Take(3).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" WHERE \"id\"=4 LIMIT 3;");
        }

        [TestMethod]
        public void OrderBy()
        {
            TestUtils.QueryFunc query = context => context.Values.OrderBy(v => v.Id).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC;");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenOrderBy()
        {
            TestUtils.QueryFunc query =
                context => context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).OrderBy(at => at.Id2).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC;");
        }

        [TestMethod]
        public void OrderByThenByDescending()
        {
            TestUtils.QueryFunc query = context => context.Values.OrderBy(v => v.Id).ThenByDescending(v2 => v2.Value).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC,\"value\" DESC;");
        }

        [TestMethod]
        public void OrderByThenOrderBy()
        {
            TestUtils.QueryFunc query = context => context.Values.OrderBy(v => v.Id).OrderByDescending(v2 => v2.Value).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC,\"value\" DESC;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeBeforeOrderBy()
        {
            TestUtils.QueryFunc query = context => context.Values.Take(4).OrderBy(v => v.Id).ToList();
            TestUtils.ExecuteQuery(query, "invalid");
        }

        [TestMethod]
        public void OrderByThenTake()
        {
            TestUtils.QueryFunc query = context => context.Values.OrderBy(v => v.Id).Take(4).ToList();
            TestUtils.ExecuteQuery(query, "SELECT \"id\",\"value\" FROM \"myvalue\" ORDER BY \"id\" ASC LIMIT 4;");
        }

        [TestMethod]
        public void SelectDistinct()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(v => v.Id).Distinct().ToList();
            TestUtils.ExecuteQuery(query, "SELECT DISTINCT \"id\" FROM \"myvalue\";");
        }

        [TestMethod]
        public void SelectDistinctTake()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(v => v.Id).Distinct().Take(3).ToList();
            TestUtils.ExecuteQuery(query, "SELECT DISTINCT \"id\" FROM \"myvalue\" LIMIT 3;");
        }

        [TestMethod]
        [ExpectedException(typeof(CqlLinqException))]
        public void SelectTakeThenDistinct()
        {
            TestUtils.QueryFunc query = context => context.Values.Select(v => v.Id).Take(3).Distinct().ToList();
            TestUtils.ExecuteQuery(query, "SELECT DISTINCT \"id\" FROM \"myvalue\" LIMIT 3;");
        }
       
    }
}