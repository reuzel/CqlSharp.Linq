﻿// CqlSharp.Linq - CqlSharp.Linq.Test
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

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class TableTest
    {
        [TestMethod]
        public void CheckTableName()
        {
            var table = new CqlTable<MyValue>(new MyContext());
            Assert.AreEqual("myvalue", table.Name, "Table name is wrong!");
        }

        [TestMethod]
        public void CheckAnnotatedTableName()
        {
            var table = new CqlTable<AnnotatedTable>(new MyContext());
            Assert.AreEqual("linqtest.myvalue", table.Name, "Table name is wrong!");
        }
        
        [TestMethod]
        public void CheckTableNameWhenKeyspaceSet()
        {
            var table = new CqlTable<MyValue>(new MyContext());
            table.Context.Database.Keyspace = "linqtest2";
            Assert.AreEqual("linqtest2.myvalue", table.Name, "Table name is wrong!");
        }

       
    }
}