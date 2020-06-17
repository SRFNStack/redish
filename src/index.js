const ObjectID = require( 'isomorphic-mongo-objectid' )
const { flatten, inflate, dateSerializer, dateDeserializer } = require( 'jpflat' )
const serializers = [ dateSerializer ]
const deserializers = [ dateDeserializer ]
const { promisify } = require( 'util' )
let cmd = null
let client = null
let mode = 'redis'
let commands = {
    'redis': {
        GETOBJ: 'HGETALL',
        SETOBJ: 'HMSET',
        DELFIELDS: 'HDEL'
    },
    'ssdb': {
        GETOBJ: 'multi_hget',
        SETOBJ: 'multi_hset',
        DELFIELDS: 'multi_hdel'
    }
}

const assertClientSet = () => {if( !client ) throw new Error( 'Client must be set before using a collection' )}

const prefixLength = 36

const toPrefix = ( pre ) => {
    if( pre.length === prefixLength ) return pre.length
    if( pre.length < prefixLength ) return pre.padEnd( prefixLength, ' ' )
    return pre.substr( 0, prefixLength )
}
const prefix = ( pre, str ) => `${toPrefix( prefixLength )}${str}`
const getPrefix = ( value ) => value.substr( 0, prefixLength )
const prefixes = {
    emptyObject: toPrefix( 'emptyObject' ),
    emptyArray: toPrefix( 'emptyArray' ),
    null: toPrefix( 'null' ),
    undefined: toPrefix( 'undefined' ),
    boolean: toPrefix( 'boolean' ),
    string: toPrefix( 'string' ),
    BigInt: toPrefix( 'BigInt' ),
    Symbol: toPrefix( 'Symbol' ),
    function: toPrefix( 'function' ),
    number: toPrefix( 'number' )
}

const prefixSerializers = {
    emptyObject: {},
    emptyArray: {},
    null: {},
    undefined: {},
    boolean: {},
    string: {},
    BigInt: {},
    Symbol: {},
    function: {},
    number: {
        canSerialize: ( o ) => typeof o === 'number',
        serialize: ( n ) => prefixes.number + n.toString(),
        canDeserialize: ( value ) => getPrefix( value ) === prefixes.number,
        deserialize: ( n ) => prefix( 'number', n.toString() )
    }
}


module.exports = {

    /**
     * Set the redis client. Created as follows:
     *
     * const redis = require("redis");
     * const client = redis.createClient();
     *
     * This is left to the user to allow auth configuration and the like
     */
    setClient( clientToSet ) {
        client = clientToSet
        cmd = promisify( client.send_command ).bind( client )
    },
    /**
     * Set the client mode to either redis or ssdb.
     *
     * Default mode is redis
     * @param newMode either "redis" or "ssdb"
     */
    setMode( newMode ) {
        if( [ 'redis', 'ssdb' ].indexOf( newMode ) < 0 )
            throw new Error( 'Invalid mode: ' + newMode )
        mode = newMode
    },
    /**
     * Redis stores all data as a "binary safe" string. This library handles primitive values and Date objects by default.
     *
     * You can add more custom serializers here. This is based on the jpflat(https://github.com/narcolepticsnowman/jpflat) library, so you must pass
     * serializers compatible with that.
     *
     *
     */
    addCustomStringSerializer( serializer ) {
        serializers.push( serializer )
    },
    /**
     * Redis stores all data as a "binary safe" string. This library handles primitive values and Date objects by default.
     *
     * You can add more custom deserializers here. This is based on the jpflat(https://github.com/narcolepticsnowman/jpflat) library, so you must pass
     * deserializers compatible with that.
     *
     *
     */
    addCustomStringDeserializer( deserializer ) {
        deserializer.push( deserializer )
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
     * TODO: Add basic single key indexing
     * TODO: Add support for "sort indexes" for speedy paging using zsets
     * TODO: Add support for unique constraints using sets
     *
     * @param key The key for this collection. A good key might be something like `${customerId}:widgets`.
     * @returns {{save(Object), findOneByKey(*), findOneBy(*,*), findAll(number=, number=)}}
     */
    collection( key ) {
        assertClientSet()
        return {
            /**
             * Save an object to redis and add it to the collection
             *
             * When saving an object these steps are followed
             * A key is generated for objects if no truthy "id" field is provided and added to the object as the field "id"
             * The object is flattened to a set of path/value pairs
             * The key is added to the collection sorted set
             *
             * Storing the keys this way instead of as a json string allows scanning and other functionality in redis that a simple string would not
             *
             * Currently empty arrays and empty sets will not be serialized because there is no way to represent them in redis, this means deserialized values will not be
             * truly equal, though in practice this is usable.
             *
             * A Transactional MULTI command is used to ensure the key add to the sorted set and the update to all of the keys succeed as a transaction.
             * @param obj The saved object
             */
            async save( obj ) {
                if( !obj || typeof obj !== 'object' )
                    throw new Error( 'You can only save objects with redish' )
                if( !obj.id ) {
                    obj.id = ObjectID().toString()
                }
                let flatObj = await flatten( obj )

                //This hkeys lookup is necessary to ensure that any keys that were deleted get reflected in the database
                //otherwise the deleted keys will get reloaded next time the object loads
                const deletedKeys = ( await cmd( 'hkeys', [ obj.id ] ) ).filter( ( obj, key ) => !flatObj.hasOwnProperty( key ) )
                await cmd(commands[mode].DELFIELDS, deletedKeys)
                await cmd( commands[ mode ].SETOBJ, [ obj.id, ...Object.entries( flatObj ).flat() ] )
                return obj
            },
            /**
             * Find one object in the collection by it's key
             *
             * This performs a HGETALL then inflates the object from it's path value pairs
             * @param id
             */
            async findOneById( id ) {
                if( !id ) throw new Error( 'You must provide a key to find' )
                return await cmd( commands[ mode ].GETOBJ, [ id ] ).then( ( res ) => inflate( res ) )
            },
            /**
             * Find all of the objects stored in this collection, one page at a time
             * @param page The page number to get
             * @param size The number of objects to retrieve at a time
             */
            async findAll( page = 0, size = 10 ) {

            },
            /**
             * Scan the collection for a object that has the specified value for the field
             * TODO: support multiple fields and complex boolean statements (foo == 5 and (bar == yep or baz == nope))
             * @param field
             * @param value
             */
            async findOneBy( field, value ) {
                //for each document in each page of the collection, scan the documents and search for matches
            }
        }
    }
}