using System;

namespace CqlSharp.Linq
{
    /// <summary>
    /// Provides access to the database underlying a context
    /// </summary>
    public class CqlDatabase : IDisposable
    {
        private readonly CqlContext _cqlContext;
        private string _connectionString;
        private CqlConnection _connection;

        public CqlDatabase(CqlContext cqlContext)
        {
            _cqlContext = cqlContext;
            CommandTimeout = 10;
        }

        /// <summary>
        /// Gets or sets the command timeout.
        /// </summary>
        /// <value>
        /// The command timeout.
        /// </value>
        public int? CommandTimeout { get; set; }

        /// <summary>
        /// Gets or sets the keyspace to use for all queries.
        /// </summary>
        /// <value>
        /// The keyspace.
        /// </value>
        public string Keyspace { get; set; }

        /// <summary>
        ///   Gets the connection string.
        /// </summary>
        /// <value> The connection string. </value>
        public string ConnectionString
        {
            get
            {
                if (_connectionString == null)
                    _connectionString = _cqlContext.GetType().Name;

                return _connectionString;
            }

            set
            {
                if (_connection != null)
                    throw new CqlLinqException("Can not change database, as the connection of the context is already opened.");

                _connectionString = value;
            }
        }

        /// <summary>
        ///   Gets or sets the log where executed CQL queries are written to
        /// </summary>
        /// <value> The log. </value>
        public Action<string> Log { get; set; }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <value>
        /// The connection.
        /// </value>
        public CqlConnection Connection
        {
            get
            {
                if (_connection == null)
                    _connection = new CqlConnection(ConnectionString);

                return _connection;
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            if (_connection != null)
                _connection.Dispose();
        }

        internal void LogQuery(string cql)
        {
            if (Log != null)
                Log(cql);
        }
    }
}