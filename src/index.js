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
     * @param client The redis client to use, this library assumes this implementation: https://www.npmjs.com/package/redis.

     * @param serializers Specify custom jpflat serializers to use for serialization. The default is to use require('./stringizer.js')
     * @param deserializers Specify custom jpflat deserializers to use for deserialization. The default is to use require('./stringizer.js')
     * @param pathReducer The path reducer to use to flatten objects. Default is json path reduce from jpflat
     * @param pathExpander The path expander to use to deserialize. Default is the stringizer pathExpander, which uses jsonpath and appends type information to the path
     * @returns {{findOneById(*), save(*=, {collectionKey?: *, idGenerator?: *, auditUser?: *}): Promise<*>, deleteById(*=, *=): Promise<void>, findAll(*, *=, *=): Promise<*>}}
     */
    createDb( client,
              serializers = [ stringizer ],
              deserializers = [ stringizer ],
              pathReducer = stringizer.pathReducer,
              pathExpander = stringizer.pathExpander ) {

        if( !client ) throw new Error( 'Client must be set before using the db' )
        let hkeys = promisify( client.hkeys ).bind( client )
        let watch = promisify( client.watch ).bind( client )
        let hgetall = promisify( client.hgetall ).bind( client )

        /**
         * Find one object in the db by it's id
         *
         * @param id
         */
        async function findOneById( id ) {
            if( !id ) throw new Error( 'You must provide an id' )
            let res = await hgetall( id )
            if( res ) {
                let inflated = await inflate( res, deserializers, pathExpander )
                if( Array.isArray( inflated ) ) {
                    inflated.id = id
                }
                return inflated
            }
            return res
        }

        async function doSave( obj, options, deleteMissingKeys) {
            let collectionKey,
                idGenerator,
                audit,
                auditUser
            if(typeof options === 'string'){
                collectionKey = options
            } else if (Array.isArray(options) || typeof options !== 'object'){
                throw new Error('Invalid save options:'+JSON.stringify(options))
            } else {
                collectionKey = options.collectionKey
                idGenerator = options.idGenerator || function(){return ObjectID().toString()}
                audit = typeof options.audit === 'boolean' ? options.audit : true
                auditUser = options.auditUser
            }

            if( !obj || typeof obj !== 'object' )
                throw new Error( 'You can only save truthy objects with redish' )
            if( Array.isArray( obj ) && obj.length === 0 )
                throw new Error( 'Empty arrays cannot be saved' )
            let isNew = !obj.id || ( audit && !obj.createdAt )
            if( isNew ) {
                if( !obj.id ) obj.id = idGenerator( obj )
                if( audit ) {
                    obj.createdAt = new Date().getTime()
                    if( auditUser ) obj.createdBy = auditUser
                }
            } else if( audit ) {
                obj.updatedAt = new Date().getTime()
                if( auditUser ) obj.updatedBy = auditUser
            }

            let flatObj = await flatten( obj, serializers, pathReducer )
            //begin transaction to ensure the zset stays consistent
            await watch( ...[obj.id, collectionKey].filter(i=>i) )
            const multi = client.multi()
            multi.hmset( obj.id, ...Object.entries( flatObj ).flat() )
            if( !isNew && deleteMissingKeys ) {
                let currentKeys = !isNew && await hkeys( obj.id ) || []
                //Get the current set of object keys and delete any keys that do not exist on the current object
                const deletedKeys = currentKeys.filter( ( key ) => !flatObj.hasOwnProperty( key ) )
                if( deletedKeys && deletedKeys.length > 0 )
                    multi.hdel( obj.id, ...deletedKeys )
            } else if(isNew) {
                if( collectionKey )
                    multi.zadd( collectionKey, 0, obj.id )
            }
            await promisify( multi.exec ).bind( multi )()
            return obj
        }

        return {
            /**
             * Save an object and optionally add it to a collection
             *
             * This treats keys missing from the object as deleted keys.
             * To avoid this behavior, use patch instead
             *
             * @param obj The saved object
             * @param options The options to use for saving this object
             * @param options.collectionKey An optional collection key
             * @param options.idGenerator A function that receives the object being saved and generates a new id for it. The default is to create a bson objectid.
             * @param options.audit Whether to enable auditing addition and management of auditing fields createdBy, createdAt, updatedBy, updatedAt
             * @param options.auditUser The user identifier to use for the "By" audit fields
             */
            //TODO add list of keys to index
            async save( obj, options = {} ) {
                return doSave( obj, options, true )
            },
            //TODO add update method that allows patching existing objects. this is to avoid treating missing keys as delete
            /**
             * Upsert an object by id
             * This is similar to save except it only sets keys and will never delete existing keys
             *
             * If an id is not provided on the object one will be generated
             *
             * @param obj The saved object
             * @param options The options to use for saving this object
             * @param options.collectionKey An optional collection key
             * @param options.idGenerator A function that receives the object being saved and generates a new id for it. The default is to create a bson objectid.
             * @param options.audit Whether to enable auditing addition and management of auditing fields createdBy, createdAt, updatedBy, updatedAt
             * @param options.auditUser The user identifier to use for the "By" audit fields
             */
            async upsert( obj, options = {} ) {
                return doSave( obj, options, false )
            },
            /**
             * Delete an object by it's id
             * @param id The object to delete
             * @param collectionKey An optional collection to remove the object's id from
             * @returns {Promise<void>}
             */
            async deleteById( id, collectionKey ) {
                await watch( ...[id, collectionKey].filter(i=>i) )

                const multi = client.multi()
                multi.del( id )
                if( collectionKey ) multi.zrem( collectionKey, id )
                await promisify( multi.exec ).bind( multi )()
            },
            /**
             * Find all of the objects stored in this collection, one page at a time
             * @param collectionKey The collection to find objects in
             * @param page The page number to get
             * @param size The number of objects to retrieve at a time
             */
            async findAll( collectionKey, page = 0, size = 10 ) {
                const ids = await new Promise( ( resolve, reject ) => {
                    let start = page * size
                    let end = start + size - 1
                    client.zrange( collectionKey, start, end, ( err, res ) => {
                        if( err ) reject( err )
                        else resolve( res )
                    } )
                } )
                if( ids && ids.length ) {
                    return await Promise.all( ids.map( id => findOneById( id ) ) )
                }
                return []
            },
            // /**
            //  * Scan the collection for a object that has the specified value for the field
            //  * TODO: support multiple fields and complex boolean statements (foo == 5 and (bar == yep or baz == nope))
            //  * @param field
            //  * @param value
            //  */
            // async findOneBy( collectionKey, field, value ) {
            //     //for each document in each page of the collection, scan the documents and search for matches
            // },
            findOneById
            //TODO implement prefix search
            //https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-1-autocomplete/6-1-2-address-book-autocomplete/
            //TODO add index backfill method for adding an index to an existing collection
        }
    }
}