const ObjectID = require( 'isomorphic-mongo-objectid' )
const { flatten, inflate } = require( 'jpflat' )
const { promisify } = require( 'util' )
let cmd = null
let client = null

const assertClientSet = () => {if( !client ) throw new Error( 'Client must be set before using a collection' )}

const getPrefix = value => value.substr( 0, 1 )
const removePrefix = value => value.substr( 1 )
const prefixes = {
    emptyObject: '0',
    emptyArray: '1',
    null: '2',
    undefined: '3',
    boolean: '4',
    string: '5',
    BigInt: '6',
    Symbol: '7',
    function: '8',
    number: '9',
    Date: 'a'
}

const stringizers = {
    [ prefixes.emptyObject ]: {
        to: () => '',
        from: () => ({})
    },
    [ prefixes.emptyArray ]: {
        to: () => '',
        from: () => []
    },
    [ prefixes.null ]: {
        to: () => '',
        from: () => null
    },
    [ prefixes.undefined ]: {
        to: () => '',
        from: () => undefined
    },
    [ prefixes.boolean ]: {
        to: o => String( o ),
        from: s => Boolean( s )
    },
    [ prefixes.string ]: {
        to: o => String( o ),
        from: s => s
    },
    [ prefixes.BigInt ]: {
        to: o => String( o ),
        from: s => BigInt( s )
    },
    [ prefixes.Symbol ]: {
        to: o => String( o ),
        from: s => Symbol.for( s.substring( 'Symbol('.length, s.length - 1 ) )
    },
    [ prefixes.function ]: {
        to: o => String( o ),
        from: s => eval( s )
    },
    [ prefixes.number ]: {
        to: o => String( o ),
        from: s => Number( s )
    },
    [ prefixes.Date ]: {
        to: o => o.toISOString(),
        from: s => new Date(s)
    }
}

const choosePrefix = o => {
    if(o === undefined) return prefixes.undefined
    if(o === null) return prefixes.null
    if(Array.isArray(o) && o.length === 0) return prefixes.emptyArray
    if(typeof o === 'object' && Object.keys(o).length === 0) return prefixes.emptyObject
    return prefixes[ typeof o ] || prefixes[ o && o.constructor.name ]
}

/**
 * This jpflat serializer/deserializer supports basic types in javascript and treats everything else as a plain string
 * @type {{serialize: (function(*=): string), canDeserialize: (function(*=): *), canSerialize: (function(*=): *), deserialize: (function(*=): *)}}
 */
const toStringizer = {
    canSerialize: o => !!choosePrefix(o),
    canDeserialize: () => true,
    serialize: o => {
        let prefix = choosePrefix(o)||prefixes.string
        return `${prefix}${stringizers[prefix].to(o)}`
    },
    deserialize: value => stringizers[ getPrefix( value ) ].from( removePrefix( value ) )
}

const serializers = [ toStringizer ]
const deserializers = [ toStringizer ]

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
     * Redis stores all data as a binary safe string. This library handles primitive values and Date objects by default.
     *
     * You can add more custom serializers here. This is based on the jpflat(https://github.com/narcolepticsnowman/jpflat) library, so you must pass
     * serializers compatible with that.
     *
     */
    addCustomStringSerializer( serializer ) {
        serializers.unshift( serializer )
    },
    /**
     * Redis stores all data as a binary safe string. This library handles primitive values and Date objects by default.
     *
     * You can add more custom deserializers here. This is based on the jpflat(https://github.com/narcolepticsnowman/jpflat) library, so you must pass
     * deserializers compatible with that.
     *
     */
    addCustomStringDeserializer( deserializer ) {
        deserializer.unshift( deserializer )
    },
    /**
     * Returns the actual array of serializers, use this to remove or modify serializers
     * @returns {*[]}
     */
    getSerializers() {
        return serializers
    },
    /**
     * Returns the actual array of deserializers, use this to remove or modify deserializers
     */
    getDeserializers() {
        return deserializers
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
             * Save an object and add it to the collection
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
                let flatObj = await flatten( obj, serializers )

                //This hkeys lookup is necessary to ensure that any keys that were deleted get reflected in the database
                //otherwise the deleted keys will get reloaded next time the object loads
                const deletedKeys = ( await cmd( 'hkeys', [ obj.id ] ) )
                    .filter( ( key ) => !flatObj.hasOwnProperty( key ) )
                await cmd('MULTI')
                if( deletedKeys && deletedKeys.length > 0 )
                    await cmd( 'HDEL', [ obj.id, ...deletedKeys ] )
                await cmd( 'HMSET', [ obj.id, ...Object.entries( flatObj ).flat() ] )
                await cmd('EXEC')
                return obj
            },
            /**
             * Find one object in the collection by it's key
             *
             * This performs a HGETALL then inflates the object from it's path value pairs
             * @param id
             */
            async findOneById( id ) {
                if( !id ) throw new Error( 'You must provide an id' )
                return await cmd( 'HGETALL', [ id ] ).then( ( res ) => res ? inflate( res, deserializers ) : res )
            }
            // /**
            //  * Find all of the objects stored in this collection, one page at a time
            //  * @param page The page number to get
            //  * @param size The number of objects to retrieve at a time
            //  */
            // async findAll( page = 0, size = 10 ) {
            //
            // },
            // /**
            //  * Scan the collection for a object that has the specified value for the field
            //  * TODO: support multiple fields and complex boolean statements (foo == 5 and (bar == yep or baz == nope))
            //  * @param field
            //  * @param value
            //  */
            // async findOneBy( field, value ) {
            //     //for each document in each page of the collection, scan the documents and search for matches
            // }
        }
    }
}