#CqlSharp.Linq

CqlSharp.Linq contains a Linq-to-Cql provider for use with the Apache Cassandra database.

Main features are:
* Translation of Linq queries to CQL select statements.
* All CQL functions are supported, as well as tokens and the allow filtering clause.
* Consistency and Paging query behaviour can be set per query via IQueryable extensions
* Linq projections (IQueryable.Select statements) can become arbitrarily complex
* Generated mapping of query results to objects is extremely fast, through compiled expressions
* Linq queries can be (pre-)compiled for increased performance
* Snapshot-based entity change tracking is supported, allowing for easy insert, change and updates of entities
* Change tracking can be switched off globally or per query
* CqlSharp.Linq provides an EntityFramework like interface

## Context and Table
CqlContext and CqlTable are the classes that form the core of the provider. CqlContext is the IQueryProvider, while CqlTable is the IQueryable.

To use the provider, subclass CqlContext and add CqlTable properties. The properties will be automatically generated/set when the subclass is created.

When no connection string is provided to the context constructor, it is assumed that the name of the subclass refers to a connection string in the app/service configuration file.

```c#
public class Blog
{
    public Guid Id {get; set;}
    public string Name {get; set;}
    public string Text {get; set;}
}

public class Comment
{
    public Guid Blog {get; set;}
    public Guid CommentId {get; set;}
    
    public string Author {get; set;}
    public string Text {get; set;}
}

public class MyBlogContext : CqlContext
{
    public CqlTable<Blog> Blogs { get; set;}
    
    public CqlTable<Comment> Comments { get; set; }
}

...

public List<string> GetCommentTexts(Guid blogid)
{
    using(var context = new MyBlogContext())
    {
        var comments = context.Comments
                        .Where(c => c.Blog == blogid)
                        .Select(c => c.Author + ": " +c.Text);
                        
        return comments.ToList();
    }
}
```

##Supported Operations
The following operations are supported:

* Select
* Where
* Any
* Count
* LongCount
* First
* FirstOrDefault
* Single
* SingleOrDefault
* ToList
* ToArray
* ToDictionary
* ToLookup
* OrderBy
* OrderByDescending
* ThenBy
* ThenByDescending
* Take
* Distinct

Note that as a result of Cassandra not supporting subqueries:

* `Take` must come after any clause that would introduce a where clause in the resulting Cql. E.g. `MyTable<T>.Take(4).First(c=>c.Id=2)` is invalid, but `MyTable<T>.Where(c=>c.Id=2).Take(4).First()` is valid.
* `Distinct` must come after any `Take` statement

All the CQL functions (including TTL and WriteTime) are supported through the CqlFunctions class:

```c#
	context.Values.Where(v => CqlFunctions.Token(v.Id) < CqlFunctions.Token(0)).ToList();

	context.Values.Where(v => v.Id == 100).Select(v => CqlFunctions.TTL(v.Value)).ToList();
```

Last, the query can be influenced through the following IQueryable extensions:

* AllowFiltering(); adds the Allow Filtering option to the query
* AsNoTracking(); switches off change tracking for the returned entities
* WithConsistency(); sets the required consistency level for the given query
* WithPageSize(); enable result set paging for queries that return large numbers of results

##Compiled Queries
Queries may be compiled for increased performance:

```c#
	Func<MyContext, Guid, string, string> compiledQuery =
       CompiledQuery.Compile<MyContext, Guid, string, string>
		(
           (context, id, append) => context.Comments
											.Where(comment => comment.CommentId == id)
											.Select(comment => comment.Value + append).First()
		);
	
	...

	string GetWithGoodComment(Guid commentId)
	{
		using (var context = new MyContext(ConnectionString))
		{
			return compiledQuery(context, commentId, "!!GOOD COMMENT!!");
		}
	}
```

##Change Tracking
CqlSharp.Linq implements snapshot based change tracking. Simply query for the entities, make some changes and 
save the changes by invoking SaveChanges on the responsible context.

```c#

	using(var context = new MyBlogContext())
    {
        var comment = context.Comments
                        .Where(c => c.Blog == blogid).First();
                        
       comment.Text = "a new comment text";

		context.SaveChanges();
    }
```

Entities can be inserted, deleted, attached or detached through the different CqlTable methods.

When a query does not contain a projection (IQueryable.Select() method invocation), the results will be automatically tracked.
This includes any entities queried through a compiled query. This tracking behaviour can be changed, by either setting the 
context.TrackChanges property to disable change tracking for the whole context, or by adding the AsNoTracking query method to
an Linq query.

Insight in the detected changes can be obtained through the context.ChangeTracker. There all tracked entities can be queried
and checked for their current state. 

Any changes will be send as part of a transaction. Transactions can be influenced through the context.Database 
CurrentTransaction property and the BeginTransaction and UseTransaction methods. Moreover, automatic acceptance of entity
changes can be influenced through the acceptChangesDuringSave parameter of the SaveChanges method.


##Design
Most of the ideas on how this provider is created stems from a blog series from Matt Warren: 
[LINQ: Building an IQueryable provider series](http://blogs.msdn.com/b/mattwar/archive/2008/11/18/linq-links.aspx)

The general concept behind this Linq provider implementation is that a regular Linq expression tree is transformed into
an expression tree that contains Cql expressions such as select, relation term, etc. Using cql specific expression types
the original query can be adapted to contain references (expressions) to column values, allowing reasoning about the meaning
of the different operations.

The final expression tree is translated into a query plan consisting of
* a cql query
* a projector mapping results from a datareader into the required result form (object or value type)
* an aggregator, translating the projected results into the required form (e.g. by taking the first item from the set)

The API as well as the change tracking functionality is largely inspired by Microsoft's EntityFramework.

##TODO
* ~~A lot of testing~~
* ~~Cql Function support (Token, TTL, ...)~~
* Database creation logic
* ~~Create/Update/Delete functionality~~
