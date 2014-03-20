using CqlSharp.Linq.Expressions;
using System;
using System.Reflection;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    /// Container of all logic to execute a Linq query
    /// </summary>
    internal class QueryPlan
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryPlan"/> class.
        /// </summary>
        /// <param name="cql">The CQL query</param>
        /// <param name="projector">The projector of the results</param>
        /// <param name="canTrackChanges"> </param>
        /// <param name="resultFunction">The result function.</param>
        public QueryPlan(string cql, Delegate projector, bool canTrackChanges, ResultFunction resultFunction)
        {
            Cql = cql;
            Projector = projector;
            CanTrackChanges = canTrackChanges;
            ResultFunction = resultFunction;
        }

        /// <summary>
        /// Gets the CQL query string.
        /// </summary>
        /// <value>
        /// The CQL query string.
        /// </value>
        public string Cql { get; private set; }

        /// <summary>
        /// Gets the projector translating database results into an object structure
        /// </summary>
        /// <value>
        /// The projector.
        /// </value>
        public Delegate Projector { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the results of the query can be tracked for changes by the used context
        /// </summary>
        /// <value>
        ///   <c>true</c> if [can track changes]; otherwise, <c>false</c>.
        /// </value>
        public bool CanTrackChanges { get; private set; }

        /// <summary>
        /// Gets the result function that, if set, aggregates the results into the required form
        /// </summary>
        /// <value>
        /// The result function.
        /// </value>
        public ResultFunction ResultFunction { get; private set; }

        /// <summary>
        /// Executes the query plan on the specified context.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <returns></returns>
        public object Execute(CqlContext context)
        {
            //get the type of the projection
            Type projectionType = Projector.Method.ReturnType;

            //log the query
            context.Database.LogQuery(Cql);

#if DEBUG
            //return default values of execution is to be skipped
            if (context.SkipExecute)
            {
                //return empty array
                if (ResultFunction == null)
                    return Array.CreateInstance(projectionType, 0);

                //return default value or null
                return projectionType.DefaultValue();
            }
#endif

            IProjectionReader reader;
            if (CanTrackChanges)
            {
                reader = (IProjectionReader)Activator.CreateInstance(
                    typeof(TrackingReader<>).MakeGenericType(projectionType),
                    BindingFlags.Instance | BindingFlags.Public, null,
                    new object[] { context, Cql, Projector },
                    null);
            }
            else
            {
                reader = (IProjectionReader)Activator.CreateInstance(
                    typeof(ProjectionReader<>).MakeGenericType(projectionType),
                    BindingFlags.Instance | BindingFlags.Public, null,
                    new object[] { context, Cql, Projector },
                    null);
            }

            if (ResultFunction != null)
                return ResultFunction.Invoke(reader.AsObjectEnumerable());

            return reader;
        }
    }
}