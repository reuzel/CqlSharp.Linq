using CqlSharp.Serialization;
using System.Collections;
using System.Linq;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    /// Defines a key from a table entry
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    internal struct ObjectKey<TObject>
    {
        /// <summary>
        /// The object from which the key is derived
        /// </summary>
        private readonly TObject _object;

        /// <summary>
        /// Initializes a new instance of the <see cref="ObjectKey{TObject}"/> struct.
        /// </summary>
        /// <param name="obj">The object.</param>
        public ObjectKey(TObject obj)
        {
            _object = obj;
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            if (!(obj is TObject)) return false;
            return Equals((TObject)obj);
        }

        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <returns>
        /// true if the specified objects are equal; otherwise, false.
        /// </returns>
        public bool Equals(TObject other)
        {
            var accessor = ObjectAccessor<TObject>.Instance;
            foreach (var column in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                if (column.ReadFunction != null)
                {
                    var valX = column.ReadFunction(_object);
                    var valY = column.ReadFunction(other);

                    if (column.CqlType == CqlType.List || column.CqlType == CqlType.Map || column.CqlType == CqlType.Set)
                    {
                        if (!TypeSystem.SequenceEqual((IEnumerable)valX, (IEnumerable)valY))
                            return false;
                    }
                    else if (!Equals(valX, valY))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Returns a hash code for the specified object.
        /// </summary>
        /// <returns>
        /// A hash code for the specified object.
        /// </returns>
        public override int GetHashCode()
        {
            int hashCode = 1;

            var accessor = ObjectAccessor<TObject>.Instance;
            foreach (var column in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                var value = column.ReadFunction(_object);
                hashCode = hashCode * 31 + (value == null ? 0 : value.GetHashCode());
            }

            return hashCode;
        }
    }
}
