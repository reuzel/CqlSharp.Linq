using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Linq.Query
{
    internal class TrackingReader<T> : IEnumerable<T>, IProjectionReader where T : class, new()
    {
        private readonly CqlContext _context;
        private readonly string _cql;
        private readonly Func<CqlDataReader, T> _projector;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProjectionReader{T}" /> class.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="cql">The CQL.</param>
        /// <param name="projector">The projector.</param>
        /// <exception cref="System.ArgumentNullException">context
        /// or
        /// cql
        /// or
        /// projector</exception>
        public TrackingReader(CqlContext context, string cql, Func<CqlDataReader, T> projector)
        {
            if (context == null) throw new ArgumentNullException("context");
            if (cql == null) throw new ArgumentNullException("cql");
            if (projector == null) throw new ArgumentNullException("projector");

            _context = context;
            _cql = cql;
            _projector = projector;
        }

        #region IEnumerable<T> Members

        /// <summary>
        ///   Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns> A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection. </returns>
        public IEnumerator<T> GetEnumerator()
        {
            var table = _context.GetTable<T>();
            var tracker = _context.MutationTracker.GetTracker(table);

            using (var connection = new CqlConnection(_context.ConnectionString))
            {
                connection.Open();

                if (_context.Log != null) _context.Log(_cql);

                var command = new CqlCommand(connection, _cql);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var value = _projector(reader);
                        yield return tracker.GetOrAdd(value);
                    }
                }
            }
        }

        /// <summary>
        ///   Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns> An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection. </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        /// <summary>
        /// Returns an enumerator that iterates through the collection, and returns the result as objects
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection.
        /// </returns>
        public IEnumerable<object> AsObjectEnumerable()
        {
            return this.Select(elem => (object)elem);
        }

    }
}