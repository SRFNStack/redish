const ObjectID = require( 'isomorphic-mongo-objectid' )
const { flatten, inflate } = require( 'jpflat' )
const { promisify } = require( 'util' )
const stringizer = require( './stringizer.js' )
module.exports = {
    /**
     * Create a db object to use for saving arbitrary objects to redis.
     *
     * Since everything in redis is stored as a string, this library injects type hints by appending them to the keys with a delimiter.
     *
     * This behavior can be overridden by specifying the pathReduce and pathExpander.
     *
     * You can also provide your own own serializer and deserializer to change the way values are converted to strings.
     *
     * TODO: Add basic single key indexing
     * TODO: Add support for "sort indexes" for speedy paging using zsets
     * TODO: Add support for unique constraints using sets
     *
     * @param client The redis client to use, client must have a send_command function like this implementation: https://www.npmjs.com/package/redis.

     * @param serializers Specify custom jpflat serializers to use for serialization. The default is to use require('./stringizer.js')
     * @param deserializers Specify custom jpflat deserializers to use for deserialization. The default is to use require('./stringizer.js')
     * @param pathReducer The path reducer to use to flatten objects. Default is json path reduce from jpflat
     * @param pathExpander The path expander to use to deserialize. Default is the stringizer pathExpander, which uses jsonpath and appends type information to the path
     * @returns {{save(Object), findOneById(*)}}
     */
    createDb( client,
              serializers = [ stringizer ],
              deserializers = [ stringizer ],
              pathReducer = stringizer.pathReducer,
              pathExpander = stringizer.pathExpander ) {

        if( !client ) throw new Error( 'Client must be set before using the db' )
        if( typeof client.send_command !== 'function' ) throw new Error( 'client must support send_command callback style function' )
        let cmd = promisify( client.send_command ).bind( client )

        return {
            /**
             * Save an object and optionally add it to a collection
             *
             * @param obj The saved object
             * @param collectionKey An optional collection key
             * @param idGenerator A function that receives the object being saved and generates a new id for it. The default is to create a bson objectid.
             */
            async save( obj, collectionKey, idGenerator = ( objToSave ) => ObjectID().toString() ) {
                if( !obj || typeof obj !== 'object' )
                    throw new Error( 'You can only save truthy objects with redish' )
                if( Array.isArray( obj ) && obj.length === 0 )
                    throw new Error( 'Empty arrays cannot be saved' )
                let isNew = !obj.id
                if( isNew ) {
                    obj.id = idGenerator( obj )
                }
                let flatObj = await flatten( obj, serializers, pathReducer )
                //begin transaction to ensure the zset stays consistent
                await cmd( 'WATCH', [ obj.id ] )
                let currentKeys = !isNew && await cmd( 'HKEYS', [ obj.id ] ) || []
                await cmd( 'MULTI' )
                await cmd( 'HMSET', [ obj.id, ...Object.entries( flatObj ).flat() ] )
                if( !isNew ) {
                    //Get the current set of object keys and delete any keys that do not exist on the current object
                    const deletedKeys = currentKeys.filter( ( key ) => !flatObj.hasOwnProperty( key ) )
                    if( deletedKeys && deletedKeys.length > 0 )
                        await cmd( 'HDEL', [ obj.id, ...deletedKeys ] )
                } else {
                    if( collectionKey ) await cmd( 'ZADD', [ collectionKey, obj.id, 0 ] )
                }
                //TODO add configurable retry
                if( await cmd( 'EXEC' ) === null )
                    throw new Error( 'Failed to update ' + obj.id + '. Object was modified during transaction.' )

                return obj
            },
            /**
             * Find one object in the db by it's id
             *
             * @param id
             */
            async findOneById( id ) {
                if( !id ) throw new Error( 'You must provide an id' )
                let res = await cmd( 'HGETALL', [ id ] )
                if( res ) {
                    let inflated = await inflate( res, deserializers, pathExpander )
                    if( Array.isArray( inflated ) ) {
                        inflated.id = id
                    }
                    return inflated
                }
                return res

            }
            // /**
            //  * Find all of the objects stored in this collection, one page at a time
            //  * @param page The page number to get
            //  * @param size The number of objects to retrieve at a time
            //  */
            // async findAll( collectionKey, page = 0, size = 10 ) {
            //
            // },
            // /**
            //  * Scan the collection for a object that has the specified value for the field
            //  * TODO: support multiple fields and complex boolean statements (foo == 5 and (bar == yep or baz == nope))
            //  * @param field
            //  * @param value
            //  */
            // async findOneBy( collectionKey, field, value ) {
            //     //for each document in each page of the collection, scan the documents and search for matches
            // }
        }
    }
}