const ObjectID = require('isomorphic-mongo-objectid')
const { flatten, inflate } = require('jpflat')
const stringizer = require('./stringizer.js')
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
  createDb (client, { valueSerializers, valueDeserializers, pathReducer, pathExpander } = {}) {
    if (!valueSerializers) valueSerializers = [stringizer]
    if (!Array.isArray(valueSerializers)) valueSerializers = [valueSerializers]
    if (!valueDeserializers) valueDeserializers = [stringizer]
    if (!Array.isArray(valueDeserializers)) valueDeserializers = [valueDeserializers]
    if (!pathReducer) pathReducer = stringizer.pathReducer
    if (typeof pathReducer !== 'function') throw new Error('pathReducer must be a function')
    if (!pathExpander) pathExpander = stringizer.pathExpander
    if (typeof pathExpander !== 'function') throw new Error('pathExpander must be a function')

    if (!client) throw new Error('Client must be set before using the db')

    async function flattenAndValidate (obj, validate, isPatch, idField) {
      const flatObj = await flatten(obj, { valueSerializers, pathReducer })
      if (validate) {
        let objToValidate = obj
        if (isPatch) {
          const existingFields = await client.hGetAll(obj[idField])
          const updated = Object.assign(existingFields, flatObj)
          objToValidate = await inflate(updated, { valueDeserializers, pathExpander })
        }
        if (!validate(objToValidate)) {
          const err = new Error(`object with id ${obj[idField]} is invalid. `)
          err.validationErrors = [].concat(validate.errors)
          throw err
        }
      }
      return flatObj
    }

    function initValidate (validate, schema, ajv, ajvOptions, ajvFormatsOptions, ajvKeywordsOptions) {
      if (typeof validate !== 'function' && schema) {
        if (ajv) {
          validate = ajv.compile(schema)
        } else {
          const defaultAjv = new Ajv(ajvOptions || undefined)
          ajvFormats(defaultAjv, ajvFormatsOptions || {})
          if (ajvKeywordsOptions) {
            ajvKeywords(defaultAjv, ajvKeywordsOptions || [])
          } else {
            ajvKeywords(defaultAjv)
          }
          validate = defaultAjv.compile(schema)
        }
      }
      return validate
    }

    async function deleteRemovedKeys (isNew, key, flatObj, multi) {
      const currentKeys = !isNew && (await client.hKeys(key) || [])
      // Get the current set of object keys and delete any keys that do not exist on the current object
      const deletedKeys = currentKeys.filter((key) => !(key in flatObj))
      if (deletedKeys && deletedKeys.length > 0) { await multi.hDel(key, ...deletedKeys) }
    }

    return {
      /**
       * A single object value identified by a single key, unassociated with any other records
       *
       * @param key The key of the object
       * @param validate A pre-compiled ajv function to use for validation.
       *                Must follow the ajv model and set an errors property on the function after detecting an invalid object.
       * @param schema A json schema to use for validation on save
       * @param idField The name of the idField to use as the unique ID. If unset, the 'id' field is used.
       *                If set, it will be prefixed with the collection key to ensure unique keys across collections.
       * @param ajv A pre-created instance of ajv to use for schema validation.
       * @param ajvOptions An Options object to initialize ajv with. Not used if schema is not set or if ajv instance is passed.
       *                   ajv-formats are installed when an ajv instance is not passed.
       * @param ajvFormatsOptions An Options object to pass to ajv-formats, ignored when ajv is set.
       * @param ajvKeywordsOptions An Options object to pass to ajv-keywords, ignored when ajv is set.
       * @returns {{load(): void, save(*, *, *): Promise<*>}|*}
       */
      singleton(key, {
        validate,
        schema,
        idField,
        ajv,
        ajvOptions,
        ajvFormatsOptions,
        ajvKeywordsOptions
      } = {}) {
        if (!key || typeof key !== 'string') {
          throw new Error('A string key must be provided')
        }
        this.key = key
        validate = initValidate(validate, schema, ajv, ajvOptions, ajvFormatsOptions, ajvKeywordsOptions)

        const doSave = async (obj, isPatch) => {
          if (!obj || typeof obj !== 'object') {
            throw new Error('Only object singletons are supported')
          }
          const flatObj = await flattenAndValidate(obj, validate, isPatch, idField)

          await client.watch([key])
          const multi = client.multi()
          await multi.hSet(key, Object.entries(flatObj))
          if (!isPatch) {
            // delete any removed keys if this is not a patch
            await deleteRemovedKeys(false, key, flatObj, multi)
          }
          await multi.exec()
          return obj
        }

        return {
          async save(obj) {
            return doSave(obj, false)
          },
          async upsert(obj) {
            return doSave(obj, true)
          },
          async load() {
            const res = await client.hGetAll(key)
            if (Object.keys(res).length === 0) {
              return null
            }
            return res && inflate(res, { valueDeserializers, pathExpander })
          }
        }
      },

      /**
       * Create a collection of objects. The items in the collection are maintained in a zset to allow finding everything and pagination
       * @param collectionKey The name of the collection. The collection key is appended to the idField to ensure unique keys across collections
       * @param validate A pre-compiled ajv function to use for validation.
       *                Must follow the ajv model and set an errors property on the function after detecting an invalid object.
       * @param schema A json schema to use for validation on save
       * @param idField The name of the idField to use as the unique ID. If unset, the 'id' field is used.
       *                If set, it will be prefixed with the collection key to ensure unique keys across collections.
       * @param ajv A pre-created instance of ajv to use for schema validation.
       * @param ajvOptions An Options object to initialize ajv with. Not used if schema is not set or if ajv instance is passed.
       *                   ajv-formats are installed when an ajv instance is not passed.
       * @param ajvFormatsOptions An Options object to pass to ajv-formats, ignored when ajv is set.
       * @param ajvKeywordsOptions An Options object to pass to ajv-keywords, ignored when ajv is set.
       * @param idGenerator A function that receives the object to save and generates a new id for it. The default is to create a bson objectid.
       * @param enableAudit Whether to enable auditing addition and management of auditing fields createdBy, createdAt, updatedBy, updatedAt
       * @param {(object) => number} calculateScore A function to calculate the score to use when adding a new object to a zset.
       *          This score determines the order records are returned in when calling findAll.
       *          The default is to return the current millis at the time of storage
       */
      collection (collectionKey, {
        validate,
        schema,
        idField,
        ajv,
        ajvOptions,
        ajvFormatsOptions,
        ajvKeywordsOptions,
        idGenerator,
        enableAudit,
        calculateScore
      } = {}) {
        if (typeof collectionKey !== 'string' || collectionKey.length < 1) {
          throw new Error('collectionKey must be a non-empty string')
        }
        if (idField && typeof idField !== 'string') {
          throw new Error('idField must be a string')
        }
        idGenerator = idGenerator || function () {
          return ObjectID().toString()
        }
        if (typeof calculateScore !== 'function') {
          calculateScore = () => new Date().getTime()
        }
        if (!idField) {
          idField = 'id'
        }

        validate = initValidate(validate, schema, ajv, ajvOptions, ajvFormatsOptions, ajvKeywordsOptions)

        const keyPrefix = `${collectionKey}__`
        const ensurePrefix = id => {
          if (typeof id !== 'string' || id.length < 1) {
            throw new Error('id must be a non-empty string')
          }
          return id.startsWith(keyPrefix) ? id : `${keyPrefix}${id}`
        }

        /**
         * Find one object in the db by its id
         *
         * @param id The id to lookup
         */
        async function findOneById (id) {
          id = ensurePrefix(id)
          const res = await client.hGetAll(id)
          if (Object.keys(res).length === 0) {
            return null
          }
          if (res) {
            const inflated = await inflate(res, { valueDeserializers, pathExpander })
            if (Array.isArray(inflated)) {
              inflated[idField] = id
            }
            return inflated
          }
          return res
        }

        async function doSave (obj, auditUser, isPatch) {
          if (!obj || typeof obj !== 'object') { throw new Error('You can only save truthy objects with redish') }
          if (Array.isArray(obj) && obj.length === 0) { throw new Error('Empty arrays cannot be saved') }
          const isNew = !obj[idField] || ( enableAudit && !obj.createdAt )
          if (isNew) {
            if (!obj[idField]) obj[idField] = `${keyPrefix}${idGenerator(obj)}`
            if (enableAudit) {
              obj.createdAt = new Date().getTime()
              if (auditUser) obj.createdBy = auditUser
            }
          } else if (enableAudit) {
            obj.updatedAt = new Date().getTime()
            if (auditUser) obj.updatedBy = auditUser
          }
          obj[idField] = ensurePrefix(obj[idField])
          const flatObj = await flattenAndValidate(obj, validate, isPatch, idField)
          // begin transaction to ensure the zset stays consistent
          await client.watch([obj[idField], collectionKey])
          const multi = client.multi()
          await multi.hSet(obj[idField], Object.entries(flatObj))
          if (!isNew && !isPatch) {
            // delete any removed keys if this is not a patch
            await deleteRemovedKeys(isNew, obj[idField], flatObj, multi)
          } else if (isNew) {
            await multi.zAdd(collectionKey, { score: calculateScore(obj), value: obj[idField] })
          }
          await multi.exec()
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
          // TODO add list of keys to index
          async save (obj, auditUser) {
            return doSave(obj, auditUser, false)
          },
          /**
           * Upsert an object by id
           * This is similar to save except it only sets keys and will never delete existing keys
           *
           * It's sort of a patch, but it will create a new record if non exists
           *
           * If an id is not provided on the object one will be generated
           *
           * @param obj The object to save
           * @param auditUser The optional user identifier to use for the "By" audit fields
           */
          async upsert (obj, auditUser) {
            return doSave(obj, auditUser, true)
          },
          /**
           * Delete an object by it's id
           * @param id The id of the object to delete
           * @returns {Promise<void>}
           */
          async deleteById (id) {
            id = ensurePrefix(id)
            const multi = client.multi()
            multi.del(id)
            multi.zRem(collectionKey, id)
            await multi.exec()
          },
          /**
           * Find all the objects stored in this collection, one page at a time
           * @param {number} page The page number to get
           * @param {number} size The number of objects to retrieve at a time
           * @param {boolean} reverse Whether to return records in reverse order
           */
          async findAll (page = 0, size = 10, reverse = false) {
            page = parseInt(page)
            size = parseInt(size)
            const start = page * size
            const end = start + size - 1
            const ids = await client.zRange(collectionKey, start, end, { REV: reverse })
            if (ids && ids.length) {
              return await Promise.all(ids.map(id => findOneById(id)))
            }
            return []
          },
          // /**
          //  * Scan the collection for an object that has the specified value for the field
          //  * TODO: support multiple fields and complex boolean statements (foo == 5 and (bar == yep or baz == nope))
          //  * @param field
          //  * @param value
          //  */
          // async findOneBy( collectionKey, field, value ) {
          //     //for each document in each page of the collection, scan the documents and search for matches
          // },
          findOneById
          // TODO implement prefix search
          // https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-1-autocomplete/6-1-2-address-book-autocomplete/
          // TODO add index backfill method for adding an index to an existing collection
        }
      }
    }
  }
}
