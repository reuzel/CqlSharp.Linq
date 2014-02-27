using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Linq.Mutations
{
    internal class MutationTracker<TEntity> where TEntity : new()
    {
        private readonly ConcurrentDictionary<TEntity, EntityEntry<TEntity>> _objects = new ConcurrentDictionary<TEntity, EntityEntry<TEntity>>(new EntityComparer<TEntity>());

        public void Attach(TEntity entity, EntityState state)
        {
            var entry = new EntityEntry<TEntity>(entity, state);
            _objects.TryAdd(entity, entry);
        }

        public bool Detach(TEntity entity)
        {
            EntityEntry<TEntity> entry;
            return _objects.TryRemove(entity, out entry);
        }

        public List<EntityEntry<TEntity>> GetChangedEntries()
        {
            return _objects.Values.Where(entry => entry.CheckForChanges()).ToList();
        }
    }
}
