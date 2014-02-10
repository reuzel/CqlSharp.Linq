// CqlSharp.Linq - CqlSharp.Linq
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

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   Provides CQL specific extensions to IQueryable
    /// </summary>
    public static class CqlQueryable
    {
        private static readonly MethodInfo AllowFilteringMethod = typeof (CqlQueryable).GetMethod("AllowFiltering");

        /// <summary>
        ///   Enables filtering of queries, using CQL's ALLOW FILTERING clause.
        /// </summary>
        /// <typeparam name="T"> </typeparam>
        /// <param name="source"> The source. </param>
        /// <returns> </returns>
        /// <exception cref="System.ArgumentNullException">source</exception>
        public static IQueryable<T> AllowFiltering<T>(this IQueryable<T> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            var method = AllowFilteringMethod.MakeGenericMethod(new[] {typeof (T)});
            var call = Expression.Call(method, source.Expression);
            return source.Provider.CreateQuery<T>(call);
        }
    }
}