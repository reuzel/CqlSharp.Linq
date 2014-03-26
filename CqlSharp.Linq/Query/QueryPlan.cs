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

using CqlSharp.Linq.Expressions;
using System;
using System.Reflection;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Container of all logic to execute a Linq query
    /// </summary>
    internal class QueryPlan
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryPlan" /> class.
        /// </summary>
        /// <param name="cql">The CQL query</param>
        /// <param name="projector">The projector of the results</param>
        /// <param name="aggregateFunction">The result function.</param>
        /// <param name="canTrackChanges">if set to <c>true</c> [can track changes].</param>
        /// <param name="consistency">The consistency.</param>
        /// <param name="batchSize">Size of the batch.</param>
        public QueryPlan(string cql, Delegate projector, AggregateFunction aggregateFunction, bool canTrackChanges, CqlConsistency? consistency, int? batchSize)
        {
            Consistency = consistency;
            PageSize = batchSize;
            Cql = cql;
            Projector = projector;
            CanTrackChanges = canTrackChanges;
            Aggregator = aggregateFunction;
        }

        /// <summary>
        /// Gets the consistency.
        /// </summary>
        /// <value>
        /// The consistency.
        /// </value>
        public CqlConsistency? Consistency { get; private set; }

        /// <summary>
        /// Gets the size of the batch.
        /// </summary>
        /// <value>
        /// The size of the batch.
        /// </value>
        public int? PageSize { get; private set; }

        /// <summary>
        ///   Gets the CQL query string.
        /// </summary>
        /// <value> The CQL query string. </value>
        public string Cql { get; private set; }

        /// <summary>
        ///   Gets the projector translating database results into an object structure
        /// </summary>
        /// <value> The projector. </value>
        public Delegate Projector { get; private set; }

        /// <summary>
        ///   Gets a value indicating whether the results of the query can be tracked for changes by the used context
        /// </summary>
        /// <value> <c>true</c> if [can track changes]; otherwise, <c>false</c> . </value>
        public bool CanTrackChanges { get; private set; }

        /// <summary>
        ///   Gets the function that, if set, aggregates the results into the required form
        /// </summary>
        /// <value> The result function. </value>
        public AggregateFunction Aggregator { get; private set; }

        /// <summary>
        ///   Executes the query plan on the specified context.
        /// </summary>
        /// <param name="context"> The context. </param>
        /// <returns> </returns>
        public object Execute(CqlContext context)
        {
            //get the type of the projection
            Type projectionType = Projector.Method.ReturnType;

#if DEBUG
            //return default values of execution is to be skipped
            if (context.SkipExecute)
            {
                //log query
                context.Database.LogQuery(Cql);

                //return empty array
                if (Aggregator == null)
                    return Array.CreateInstance(projectionType, 0);

                //return default value or null
                return projectionType.DefaultValue();
            }
#endif

            IProjectionReader reader;
            if (CanTrackChanges && context.TrackChanges)
            {
                reader = (IProjectionReader)Activator.CreateInstance(
                    typeof(TrackingReader<>).MakeGenericType(projectionType),
                    BindingFlags.Instance | BindingFlags.Public, null,
                    new object[] { context, this },
                    null);
            }
            else
            {
                reader = (IProjectionReader)Activator.CreateInstance(
                    typeof(ProjectionReader<>).MakeGenericType(projectionType),
                    BindingFlags.Instance | BindingFlags.Public, null,
                    new object[] { context, this },
                    null);
            }

            if (Aggregator != null)
                return Aggregator.Invoke(reader.AsObjectEnumerable());

            return reader;
        }
    }
}