const ObjectID = require( 'isomorphic-mongo-objectid' )
const { flatten, inflate } = require( 'jpflat' )
const { promisify } = require( 'util' )
const stringizer = require( './stringizer.js' )
const Ajv = require('ajv')
const ajvFormats = require('ajv-formats')
const ajvKeywords = require('ajv-keywords')

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
     * TODO: Add support for "sort indexes" for speedy paging using zsets on arbitrary keys
     * TODO: Add support for unique constraints using sets
     *
     * @param client The redis client to use, this library assumes this implementation: https://www.npmjs.com/package/redis.

     * @param valueSerializers Specify custom jpflat valueSerializers to use for serialization. The default is to use require('./stringizer.js')
     * @param valueDeserializers Specify custom jpflat valueDeserializers to use for deserialization. The default is to use require('./stringizer.js')
     * @param pathReducer The path reducer to use to flatten objects. Default is json path reduce from jpflat
     * @param pathExpander The path expander to use to deserialize. Default is the stringizer pathExpander, which uses jsonpath and appends type information to the path
     */
    createDb( client, {valueSerializers, valueDeserializers, pathReducer, pathExpander} = {} ) {
        if(!valueSerializers) valueSerializers = [stringizer]
        if(!Array.isArray(valueSerializers)) valueSerializers =[valueSerializers];
        if(!valueDeserializers) valueDeserializers = [stringizer]
        if(!Array.isArray(valueDeserializers)) valueDeserializers =[valueDeserializers];
        if(!pathReducer) pathReducer = stringizer.pathReducer
        if(typeof pathReducer !== 'function') throw 'pathReducer must be a function'
        if(!pathExpander) pathExpander = stringizer.pathExpander
        if(typeof pathExpander !== 'function') throw 'pathExpander must be a function'

        if( !client ) throw new Error( 'Client must be set before using the db' )
        let hkeys = promisify( client.hkeys ).bind( client )
        let watch = promisify( client.watch ).bind( client )
        let hgetall = promisify( client.hgetall ).bind( client )

        return {
            /**
             * Create a collection of objects. The items in the collection are maintained in a zset to allow finding everything and pagination
             * @param collectionKey The name of the collection. The collection key is appended to the idField to ensure unique keys across collections
             * @param schema A json schema to use for validation on save
             * @param idField The name of the idField to use as the unique Id. If unset, the 'id' field is used.
             *                If set, it will be prefixed with the collection key to ensure unique keys across collections.
             * @param ajv A pre-created instance of ajv to use for schema validation.
             * @param ajvOptions An Options object to initialize ajv with. Not used if schema is not set or if ajv instance is passed.
             *                   ajv-formats are installed when an ajv instance is not passed.
             * @param ajvFormatsOptions An Options object to pass to ajv-formats. ajv-formats is not installed and these options are not used if an ajv instance is passed.
             * @param ajvKeywordsOptions An Options object to pass to ajv-keywords. ajv-keywords is not installed and these options are not used if an ajv instance is passed.
             * @param idGenerator A function that receives the object being saved and generates a new id for it. The default is to create a bson objectid.
             * @param enableAudit Whether to enable auditing addition and management of auditing fields createdBy, createdAt, updatedBy, updatedAt
             */
            collection(collectionKey, {schema, idField, ajv, ajvOptions, ajvFormatsOptions, ajvKeywordsOptions, idGenerator, enableAudit} = {}){

                if(typeof collectionKey !== 'string' || collectionKey.length<1){
                    throw new Error('collectionKey must be a non-empty string')
                }
                if(idField && typeof idField !== 'string'){
                    throw new Error('idField must be a string')
                }
                idGenerator = idGenerator || function() {
                    return ObjectID().toString()
                }
                if(!idField) {
                    idField = 'id'
                }
                let validate
                if(schema){
                    if(ajv){
                        validate = ajv.compile(schema)
                    } else {
                        let defaultAjv = new Ajv( ajvOptions || undefined );
                        ajvFormats(defaultAjv, ajvFormatsOptions || {})
                        ajvKeywords(defaultAjv, ajvKeywordsOptions || [])
                        validate = defaultAjv.compile( schema )
                    }
                }
                const keyPrefix = `${collectionKey}__`
                const ensurePrefix = id => {
                    if( typeof id !== 'string' || id.length < 1){
                        throw new Error('id must be a non-empty string')
                    }
                    return id.startsWith(keyPrefix) ? id : `${keyPrefix}${id}`
                }
                /**
                 * Find one object in the db by it's id
                 *
                 * @param id The id to lookup
                 */
                async function findOneById( id ) {
                    id = ensurePrefix(id)
                    let res = await hgetall( id )
                    if( res ) {
                        let inflated = await inflate( res, { valueDeserializers, pathExpander } )
                        if( Array.isArray( inflated ) ) {
                            inflated[idField] = id
                        }
                        return inflated
                    }
                    return res
                }

                async function doSave( obj, auditUser, isPatch ) {
                    if( !obj || typeof obj !== 'object' )
                        throw new Error( 'You can only save truthy objects with redish' )
                    if( Array.isArray( obj ) && obj.length === 0 )
                        throw new Error( 'Empty arrays cannot be saved' )
                    let isNew = !obj[idField] || ( enableAudit && !obj.createdAt )
                    if( isNew ) {
                        if( !obj[idField] ) obj[idField] = `${keyPrefix}${idGenerator( obj )}`
                        if( enableAudit ) {
                            obj.createdAt = new Date().getTime()
                            if( auditUser ) obj.createdBy = auditUser
                        }
                    } else if( enableAudit ) {
                        obj.updatedAt = new Date().getTime()
                        if( auditUser ) obj.updatedBy = auditUser
                    }
                    obj[idField] = ensurePrefix(obj[idField])
                    let flatObj = await flatten( obj, { valueSerializers, pathReducer } )
                    if(validate){
                        let objToValidate = obj
                        if(isPatch) {
                            const existingFields = await hgetall( obj[idField] )
                            const updated = Object.assign(existingFields, flatObj)
                            objToValidate = await  inflate( updated, { valueDeserializers, pathExpander } )
                        }
                        if( !validate(objToValidate) ) {
                            const err = new Error(`object with id ${obj[idField]} being saved to collection ${collectionKey} is invalid. `)
                            err.validationErrors = [].concat(validate.errors)
                            throw err
                        }
                    }

                    //begin transaction to ensure the zset stays consistent
                    await watch( [obj[idField], collectionKey].filter( i => !!i ) )
                    const multi = client.multi()
                    multi.hmset( obj[idField], ...Object.entries( flatObj ).flat() )
                    if( !isNew && !isPatch ) {
                        //delete any removed keys if this is not a patch
                        let currentKeys = !isNew && await hkeys( obj[idField] ) || []
                        //Get the current set of object keys and delete any keys that do not exist on the current object
                        const deletedKeys = currentKeys.filter( ( key ) => !flatObj.hasOwnProperty( key ) )
                        if( deletedKeys && deletedKeys.length > 0 )
                            multi.hdel( obj[idField], ...deletedKeys )
                    } else if( isNew ) {
                        multi.zadd( collectionKey, 0, obj[idField] )
                    }
                    await promisify( multi.exec ).bind( multi )()
                    return obj
                }
                return {
                    /**
                     * Save an object and optionally add it to a collection
                     *
                     * This treats keys missing from the object as deleted keys.
                     * To avoid this behavior, use upsert instead
                     *
                     * @param obj The object to save
                     * @param auditUser The optional user identifier to use for the "By" audit fields
                     */
                    //TODO add list of keys to index
                    async save( obj, auditUser ) {
                        return doSave( obj, auditUser, false )
                    },
                    /**
                     * Upsert an object by id
                     * This is similar to save except it only sets keys and will never delete existing keys
                     *
                     * If an id is not provided on the object one will be generated
                     *
                     * @param obj The object to save
                     * @param auditUser The optional user identifier to use for the "By" audit fields
                     */
                    async upsert( obj, auditUser ) {
                        return doSave( obj, auditUser, true )
                    },
                    /**
                     * Delete an object by it's id
                     * @param id The id of the object to delete
                     * @returns {Promise<void>}
                     */
                    async deleteById( id ) {
                        id = ensurePrefix(id)
                        const multi = client.multi()
                        multi.del( id )
                        multi.zrem( collectionKey, id )
                        await promisify( multi.exec ).bind( multi )()
                    },
                    /**
                     * Find all of the objects stored in this collection, one page at a time
                     * @param page The page number to get
                     * @param size The number of objects to retrieve at a time
                     */
                    async findAll( page = 0, size = 10 ) {
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
    }
}