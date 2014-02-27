using System;
using System.Collections;
using System.Collections.Generic;
using CqlSharp.Serialization;

namespace CqlSharp.Linq.Mutations
{
    internal class EntityEntry<TEntity> where TEntity: new()
    {
        private readonly TEntity _orginal;
        private readonly TEntity _object;
        private List<ICqlColumnInfo> _changedColumns;

        public EntityEntry(TEntity entity, EntityState state)
        {
            State = state;
            _object = entity;
            
            _orginal = new TEntity();
            var accessor = ObjectAccessor<TEntity>.Instance;
            foreach(var column in accessor.NormalColumns)
            {
                if(column.ReadFunction!=null && column.WriteFunction!=null)
                {
                    var value = column.ReadFunction(entity);

                    //if the column is a collection type, clone the collection
                    if(column.CqlType == CqlType.List || column.CqlType == CqlType.Map || column.CqlType == CqlType.Set)
                    {
                        value = Activator.CreateInstance(column.Type, value);
                    }

                    column.WriteFunction(_orginal, value);
                }
            }
        }

        public EntityState State { get; set; }

        public bool CheckForChanges()
        {
            if (State == EntityState.Added || State == EntityState.Removed)
                return true;

            _changedColumns = new List<ICqlColumnInfo>();
            var accessor = ObjectAccessor<TEntity>.Instance;
            foreach (var column in accessor.NormalColumns)
            {
                if (column.ReadFunction != null)
                {
                    var original = column.ReadFunction(_orginal);
                    var actual = column.ReadFunction(_object);

                    if (column.CqlType == CqlType.List || column.CqlType == CqlType.Map || column.CqlType == CqlType.Set)
                    {
                        if (!TypeSystem.SequenceEqual((IEnumerable)original, (IEnumerable)actual))
                            _changedColumns.Add(column);
                    }
                    else if (!Equals(original, actual))
                    {
                        _changedColumns.Add(column);
                    }
                }
            }

            if (_changedColumns.Count > 0)
            {
                State = EntityState.Changed;
                return true;
            }

            return false;
        }

        

    }
}