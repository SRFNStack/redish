module.exports = {
    /**
     * A collection is a logical grouping of objects of similarish type.
     *
     * Collections should generally be used at as small a scale as possible. For instance, a single user should have
     * their own collection of widgets instead of a single collection of widgets that has every users data in it.
     * Doing this will ensure that find operations and the like will remain speedy without the need for an index.
     *
     * Complex find operations that involve searching multiple collections can be achieved using the redisearch module.
     *
     * @param key
     * @returns {{save(Object), findOne(*, *), findAll(*=, *=)}}
     */
    collection(key) {
        return {
            /**
             * Save an object to redis and add it to the collection
             *
             * When saving an object these steps are followed
             * A transaction is started in redis
             * A key is generated for objects if no truthy "key" or "id" field is provided
             * The key is added to the collection sorted set
             * The object is flattened to a set of path/value pairs
             * The values are all set in a batch call
             * The transaction is committed
             * @param obj
             */
            save(obj) {

            },
            /**
             * Find one value in the collection
             * @param key
             * @param value
             */
            findOne(key, value) {

            },
            /**
             * keys for objects in collections are stored in
             * @param page
             * @param lastId
             */
            findAll(page = 0, lastId = null) {

            },

        }
    }
}