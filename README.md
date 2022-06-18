## redish

#### What is redish 
redish is a NO->RM (Nice Object to Redis protocol Mapper) that will save your documents to a storage backend that supports
the redis protocol with transactions ( MULTI, EXEC, etc ). Redis or ( Titan + TiKV ) are excellent options.

The primary goal is to provide a typical ORM like experience that uses the redis protocol instead of sql. 

Redish adds collections, which are much like mongo collections, or tables in RDBMS. 

It supports storing and retrieving objects with the data types of properties preserved on retrieval.

## Getting Started
Provide a redis client to initialize the db.

You should use  https://www.npmjs.com/package/redis version 4+.

version 3 will not work for 0.3+ of redish.

Serialization configuration is done once per db. You can create multiple instances of db if you wish.

After creating the db, use the result to create collections. A collection consists of a key prefix and a zset that all keys
are stored in. The zset allows finding all the objects in the collection and provides speedy pagination.

Use the collection object to save, find, and update the collection and objects within it.

By default, a field named 'id' is used as the key to store the object in redis. 

The value of id is prefixed with the collection name and two underscores, i.e. given id='1234', it will store the object using the key stuff__1234.

You can change the id field by passing the option idField to db.collection. I.e. `db.collection('stuff', {idField:'myField'})`

The collection prefix is used for two reasons, first to quickly identify which collection any given key belongs too, and to
ensure uniqueness across collections since there's only a single keyspace in redis.

When loading objects, the key can be passed in with or without the prefix to make some use cases simpler. 
```js
const db = redish.createDb( client )
const stuff = db.collection('stuff')

const saved = stuff.save({
    id: '1234',
    things: [
        {name: 'thing1'},
        {name: 'thing2'}
    ]
})

expect(saved.id).toEqual('stuff__1234')

const loaded = stuff.findOneById(saved.id)

```

## Serialization
Objects are serialized using the jpflat and all fields are serialized to string

### Custom Serialization
All data in redis is stored as a string, so serialization is done by checking the type of the value, and adding a 
prefix of fixed length to every value. This way the data  

If you want to change the way that values are serialized and deserialized, you can provide custom serializers.
This is useful for cases where you want to convert objects of a specific type (i.e. Date) to a string and back.
