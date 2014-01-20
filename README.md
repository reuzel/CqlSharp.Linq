CqlSharp.Linq
=============

CQLSharp.Linq - Linq provider for querying Cassandra using CqlSharp


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
* OrderBy
* OrderByDescending
* ThenBy
* ThenByDescending
* Take

Note that, as a result of Cassandra not supporting subqueries, `Take` must come after any clause that would introduce a where clause in the resulting Cql. E.g. `MyTable<T>.Take(4).First(c=>c.Id=2)` is invalid, but `MyTable<T>.Where(c=>c.Id=2).Take(4).First()` is valid.

##Design
Most of the ideas on how this provider is created stems from a blog series from Matt Warren: 
[LINQ: Building an IQueryable provider series](http://blogs.msdn.com/b/mattwar/archive/2008/11/18/linq-links.aspx)

The general concept behind this Linq provider implementation is that a regular Linq expression tree is transformed into an expression tree that contains Cql expressions such as select, relation term, etc. Using cql specific expression types the original query can be adapted to contain references (expressions) to column values, allowing reasoning about the meaning of the different operations.

The final expression tree is translated into a Cql query as well as delegate that transforms the query
results into the required object structure.

##TODO
* A lot of testing
* Cql Function support (Token, TTL, ...)
* Database creation logic
* Create/Update/Delete functionality
