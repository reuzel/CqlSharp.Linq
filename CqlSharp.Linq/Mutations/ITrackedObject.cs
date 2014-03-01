namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    /// Non-Generic interface to manage tracked objects accross tables.
    /// </summary>
    internal interface ITrackedObject
    {
        /// <summary>
        /// Detects the changes.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="CqlLinqException">Illegal change detected: A tracked object has changed its key</exception>
        bool DetectChanges();

        /// <summary>
        /// Gets the DML statement.
        /// </summary>
        /// <returns></returns>
        string GetDmlStatement();

    }
}