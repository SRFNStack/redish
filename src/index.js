let client = null

const assertClientSet = ()=>{if(!!client) throw new Error("Client must be set before using a collection")}

module.exports = {

    /**
     * Set the redis client. Created as follows:
     *
     * const redis = require("redis");
     * const client = redis.createClient();
     *
     * This is left to the user to allow auth configuration and the like
     */
    setClient(clientToSet){
        client = clientToSet
    },
    /**
     * A collection is a logical grouping of objects of similarish type.
     *
     * Collections should be as small as possible. For instance, a single user should have
     * their own collection of widgets instead of a single collection of widgets that has every users data in it.
     * Doing this will ensure that find operations and the like will remain speedy without the need for an index.
     *
     * Complex find operations that involve searching multiple collections or large collections can be achieved using the redisearch module
     *
     * TODO: Add helpers for redisearch operations and indexing
     *
     * @param key
     * @returns {{save(Object), findOneByKey(*), findOneBy(*,*), findAll(number=, number=)}}
     */
    collection(key) {
        assertClientSet()
        return {
            /**
             * Save an object to redis and add it to the collection
             *
             * When saving an object these steps are followed
             * A key is generated for objects if no truthy "key" or "id" field is provided and added to the object as the field "key"
             * The object is flattened to a set of path/value pairs
             * The key is added to the collection sorted set
             *
             * Currently empty arrays and empty sets will not be serialized because there is no way to represent them in redis, this means deserialized values will not be
             * truly equal, though in practice this is usable.
             *
             * A Transactional MULTI command is used to ensure the key add to the sorted set and the update to all of the keys succeed as a transaction.
             * @param obj The saved object
             */
            save(obj) {


            },
            /**
             * Find one object in the collection by it's key
             *
             * This performs a HGETALL then inflates the object from it's path value pairs
             * @param key
             */
            findOneByKey(key) {

            },
            /**
             * Find all of the objects stored in this collection, one page at a time
             * @param page The page number to get
             * @param size The number of objects to retrieve at a time
             */
            findAll(page = 0, size = 10) {

            },
            /**
             * Scan the collection for a object that has the specified value for the field
             * TODO: support multiple fields and complex boolean statements (foo == 5 and (bar == yep or baz == nope))
             * @param field
             * @param value
             */
            findOneBy(field, value){
                //for each document in each page of the collection, scan the documents and search for matches
            }
        }
    }
}