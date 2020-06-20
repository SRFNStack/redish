## redish



#### What is redish 
redish is a NORM (Nice Object to Redis protocol Mapper) that will save your documents to a storage backend that supports
the redis protocol with transactions ( MULTI, EXEC, etc ). Redis or ( Titan + TiKV ) are excellent options.

It's primary goal is to provide a typical ORM like experience that uses the redis protocol instead of sql. 

Id adds collections, which are much like mongo collections, or tables in RDBMS. 

It supports storing and retrieving objects with datatypes of properties preserved.

## Serialization
Objects are serialized using the ()[]

### Custom Serialization
All data in redis is stored as a string, so serialization is done by checking the type of the value, and adding a 
prefix of fixed length to every value. This way the data  

If you want to change the way that values are serialized and deserialized, you can provide custom serializers.
This is useful for cases where you want to convert objects of a specific type (i.e. Date) to a string and back.
