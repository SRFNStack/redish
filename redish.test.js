const redish = require('./src/index.js')
const ObjectID = require('isomorphic-mongo-objectid')
const stringizer = require('./src/stringizer.js')
const Ajv = require('ajv')

const cmdRes = {
  hDel: ['ok'],
  hKeys: [[]],
  hSet: ['ok'],
  hGet: [{}],
  multi: ['ok'],
  zAdd: ['ok'],
  exec: ['ok'],
  hGetAll: [],
  zRange: []
}
const getResponse = (cmd) => {
  if (cmdRes[cmd]) {
    if (cmdRes[cmd].length > 1) {
      return cmdRes[cmd].pop()
    } else {
      return cmdRes[cmd][0]
    }
  } else {
    return null
  }
}
const mockMulti = {
  exec: jest.fn(),
  hSet: jest.fn(),
  hDel: jest.fn(),
  zAdd: jest.fn(),
  del: jest.fn(),
  zRem: jest.fn()
}

const mockClient = {
  watch: jest.fn(),
  hKeys: jest.fn(() => getResponse('hKeys')),
  hGetAll: jest.fn(() => getResponse('hGetAll')),
  zRange: jest.fn(() => getResponse('zRange')),
  multi: jest.fn(() => mockMulti)
}

const db = redish.createDb(mockClient)

const foo = db.collection('foo')
const audit = db.collection('audit', { enableAudit: true })
const scheme = db.collection('scheme', {
  schema: {
    type: 'object',
    properties: {
      name: { type: 'string' },
      favoriteColor: { type: 'string' }
    },
    required: ['name']
  }
})
const allTypes = {
  emptyObject: {},
  emptyArray: [],
  emptyString: '',
  null: null,
  undefined,
  boolean: true,
  string: 'string',
  BigInt: BigInt('123456789123456789'),
  Symbol: Symbol.for('symbol'),
  number: 100,
  Date: new Date()
}

afterEach(() => {
  jest.clearAllMocks()
})

describe('save', () => {
  it('can only save truthy objects', async () => {
    for (const badValue of [null, undefined, false, '', 5, -10, NaN]) {
      await expect(foo.save(badValue)).rejects.toThrow('You can only save truthy objects with redish')
    }
  })

  it('prefixes the id with the collection if set', async () => {
    await foo.save({ id: 'unique' })
    expect(mockMulti.hSet.mock.calls[0]).toEqual(['foo__unique', [['$.id' + ':' + stringizer.typeKeys.string, 'foo__unique']]])
  })

  it('generates an object id hex string if id is not set', async () => {
    const result = await foo.save({})
    const objectId = result.id.split('foo__')[1]
    expect(ObjectID(objectId).toString()).toBe(objectId)
    expect(mockMulti.hSet.mock.calls[0]).toEqual([result.id, [['$.id' + ':' + stringizer.typeKeys.string, result.id]]])
  })

  it('sends hDel command when keys are deleted from an existing object', async () => {
    cmdRes.hKeys.push(['$.id' + ':' + stringizer.typeKeys.string, '$.foo'])
    await foo.save({ id: 'id' })
    expect(mockMulti.hDel.mock.calls[0]).toEqual(['foo__id', '$.foo'])
  })

  it('doesn\'t send hDel command when using upsert to update an existing object', async () => {
    cmdRes.hKeys.push(['$.id' + ':' + stringizer.typeKeys.string, '$.foo'])
    await foo.upsert({ id: 'id' })
    expect(mockMulti.hDel.mock.calls.length).toEqual(0)
  })

  it('watches the keys if it needs to delete fields to ensure consistent updates', async () => {
    cmdRes.hKeys.push(['$.id' + ':' + stringizer.typeKeys.string, '$.foo'])
    await foo.save({ id: 'id' })
    expect(mockClient.watch.mock.calls[0][0]).toEqual(['foo__id', 'foo'])
  })

  it('does not send hKeys or hDel commands if the object is new', async () => {
    await foo.save({})
    expect(mockClient.hKeys.mock.calls.length).toStrictEqual(0)
    expect(mockMulti.hDel.mock.calls.length).toStrictEqual(0)
  })

  it('does not send hDel command if no keys were deleted', async () => {
    cmdRes.hKeys.push(['$.id' + ':' + stringizer.typeKeys.string, '$.foo' + ':' + stringizer.typeKeys.string])
    await foo.save({ id: 'id', foo: 'foo' })
    expect(mockMulti.hDel.mock.calls.length).toStrictEqual(0)
  })

  it('adds the objects id to the collection\'s zset with a score of 0 if it\'s a new object', async () => {
    const result = await foo.save({})
    expect(mockMulti.zAdd.mock.calls[0]).toEqual(['foo', expect.objectContaining({ value: result.id })])
    expect(mockMulti.zAdd.mock.calls[0][1].score).toBeGreaterThan(0)
  })

  it('saves array root objects correctly', async () => {
    const result = await foo.save([5, 's'])
    const objectId = result.id.split('foo__')[1]
    expect(ObjectID(objectId).toString()).toBe(objectId)
    expect(mockMulti.hSet.mock.calls[0])
      .toEqual([
        result.id, [
          ['$[0]' + ':' + stringizer.typeKeys.number, '5'],
          ['$[1]' + ':' + stringizer.typeKeys.string, 's'],
          ['$.id' + ':' + stringizer.typeKeys.string, result.id]
        ]
      ])
  })

  it('serializes types correctly', async () => {
    const result = await foo.save({ ...allTypes })
    expect(mockMulti.hSet.mock.calls[0])
      .toEqual([
        result.id, [
          ['$.emptyObject' + ':' + stringizer.typeKeys.emptyObject, '{}'],
          ['$.emptyArray' + ':' + stringizer.typeKeys.emptyArray, '[]'],
          ['$.emptyString' + ':' + stringizer.typeKeys.emptyString, '\'\''],
          ['$.null' + ':' + stringizer.typeKeys.null, 'null'],
          ['$.undefined' + ':' + stringizer.typeKeys.undefined, 'undefined'],
          ['$.boolean' + ':' + stringizer.typeKeys.boolean, 'true'],
          ['$.string' + ':' + stringizer.typeKeys.string, 'string'],
          ['$.BigInt' + ':' + stringizer.typeKeys.BigInt, '123456789123456789'],
          ['$.Symbol' + ':' + stringizer.typeKeys.Symbol, 'Symbol(symbol)'],
          ['$.number' + ':' + stringizer.typeKeys.number, '100'],
          ['$.Date' + ':' + stringizer.typeKeys.Date, allTypes.Date.toISOString()],
          ['$.id' + ':' + stringizer.typeKeys.string, result.id]
        ]])
  })

  it('serializes nested arrays correctly', async () => {
    const result = await foo.save([[[[0, { foo: [[[1]]] }]]]])
    expect(mockMulti.hSet.mock.calls[0])
      .toEqual([
        result.id, [
          ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number, '0'],
          ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number, '1'],
          ['$.id' + ':' + stringizer.typeKeys.string, result.id]
        ]])
  })

  it('serializes nested objects correctly', async () => {
    const result = await foo.save({ a: { a: { a: { a: 0, b: { b: [0, { c: 'd' }] } } } } })
    expect(mockMulti.hSet.mock.calls[0])
      .toEqual([
        result.id, [
          ['$.a.a.a.a' + ':' + stringizer.typeKeys.number, '0'],
          ['$.a.a.a.b.b[0]' + ':' + stringizer.typeKeys.number, '0'],
          ['$.a.a.a.b.b[1].c' + ':' + stringizer.typeKeys.string, 'd'],
          ['$.id' + ':' + stringizer.typeKeys.string, result.id]
        ]])
  })

  it('sets the audit fields on new objects correctly', async () => {
    const result = await audit.save({}, 'me')
    expect(new Date(result.createdAt).getTime() > 0).toStrictEqual(true)
    expect(result.createdBy).toStrictEqual('me')
    expect(result.updatedAt).toStrictEqual(undefined)
    expect(result.updatedBy).toStrictEqual(undefined)
  })

  it('sets the audit fields on existing objects correctly', async () => {
    const theDate = (new Date().getTime() - 100000)
    const result = await audit.save({ id: '1234', createdAt: theDate, createdBy: 'me' }, 'it')
    expect(result.createdAt).toStrictEqual(theDate)
    expect(result.createdBy).toStrictEqual('me')
    expect(new Date(result.updatedAt).getTime() > 0).toStrictEqual(true)
    expect(result.updatedBy).toStrictEqual('it')
  })

  it('validates the object using the provided schema', async () => {
    await scheme.save({ id: 'unique', name: 'taco', favoriteColor: 'green' })
    expect(mockMulti.hSet.mock.calls[0]).toEqual(['scheme__unique', [
      ['$.id' + ':' + stringizer.typeKeys.string, 'scheme__unique'],
      ['$.name:6', 'taco'],
      ['$.favoriteColor:6', 'green']
    ]])
  })
  it('throws an error with messages if the object isn\'t valid per the schema', async () => {
    try {
      await scheme.save({ id: 'unique', favoriteColor: 'green' })
      fail('save should\'ve thrown')
    } catch (e) {
      expect(e.validationErrors).toEqual([{
        instancePath: '',
        schemaPath: '#/required',
        keyword: 'required',
        params: {
          missingProperty: 'name'
        },
        message: 'must have required property \'name\''
      }])
    }
  })

  it('uses ajvOptions if passed', async () => {
    const ajvOptions = db.collection('ajvOptions', {
      ajvOptions: { strictNumbers: true },
      schema: {
        type: 'object',
        properties: {
          number: { type: 'number' }
        },
        required: ['number']
      }
    })
    try {
      await ajvOptions.save({ id: 'unique', number: '1234' })
      fail('save should\'ve thrown')
    } catch (e) {
      expect(e.validationErrors).toEqual([{
        instancePath: '/number',
        schemaPath: '#/properties/number/type',
        keyword: 'type',
        params: {
          type: 'number'
        },
        message: 'must be number'
      }])
    }
  })

  it('uses ajv if passed', async () => {
    const ajvInst = db.collection('ajv', {
      ajv: new Ajv(
        { strictNumbers: true }
      ),
      schema: {
        type: 'object',
        properties: {
          number: { type: 'number' }
        },
        required: ['number']
      }
    })
    try {
      await ajvInst.save({ id: 'unique', number: '1234' })
      fail('save should\'ve thrown')
    } catch (e) {
      expect(e.validationErrors).toEqual([{
        instancePath: '/number',
        schemaPath: '#/properties/number/type',
        keyword: 'type',
        params: {
          type: 'number'
        },
        message: 'must be number'
      }])
    }
  })
})

describe('findOneById', () => {
  it('should require a non empty string id is passed', async () => {
    for (const badValue of [null, undefined, false, '', 0, NaN]) {
      await expect(foo.findOneById(badValue)).rejects.toThrow('id must be a non-empty string')
    }
  })

  it('deserializes types correctly', async () => {
    const id = ObjectID().toString()
    const origDate = allTypes.Date
    const foundHash = {
      ['$.emptyObject' + ':' + stringizer.typeKeys.emptyObject]: '{}',
      ['$.emptyArray' + ':' + stringizer.typeKeys.emptyArray]: '[]',
      ['$.emptyString' + ':' + stringizer.typeKeys.emptyString]: '\'\'',
      ['$.null' + ':' + stringizer.typeKeys.null]: 'null',
      ['$.undefined' + ':' + stringizer.typeKeys.undefined]: 'undefined',
      ['$.boolean' + ':' + stringizer.typeKeys.boolean]: 'true',
      ['$.string' + ':' + stringizer.typeKeys.string]: 'string',
      ['$.BigInt' + ':' + stringizer.typeKeys.BigInt]: '123456789123456789',
      ['$.Symbol' + ':' + stringizer.typeKeys.Symbol]: 'Symbol(symbol)',
      ['$.number' + ':' + stringizer.typeKeys.number]: '100',
      ['$.Date' + ':' + stringizer.typeKeys.Date]: origDate.toISOString(),
      ['$.id' + ':' + stringizer.typeKeys.string]: id
    }

    cmdRes.hGetAll.push(foundHash)
    const result = await foo.findOneById(id)

    expect(result.emptyObject).toStrictEqual({})
    expect(result.emptyArray).toStrictEqual([])
    expect(result.emptyString).toStrictEqual('')
    expect(result.null).toStrictEqual(null)
    expect(result.undefined).toStrictEqual(undefined)
    expect(result.boolean).toStrictEqual(true)
    expect(result.string).toStrictEqual('string')
    expect(result.BigInt).toStrictEqual(BigInt('123456789123456789'))
    expect(result.Symbol).toStrictEqual(Symbol.for('symbol'))
    expect(result.number).toStrictEqual(100)
    expect(result.Date).toStrictEqual(origDate)
    expect(result.id).toStrictEqual(id)
  })

  it('deserializes nested arrays correctly', async () => {
    const saved = await foo.save([[[[0, { foo: [[[1]]] }]]]], { audit: false })
    cmdRes.hGetAll.push({
      ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number]: '0',
      ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number]: '1',
      ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
    })

    const found = await foo.findOneById(saved.id)
    expect(saved).toStrictEqual(found)
  })

  it('deserializes nested objects correctly', async () => {
    const saved = await foo.save({ a: { a: { a: { a: 0, b: { b: [0, { c: 'd' }] } } } } }, { audit: false })
    cmdRes.hGetAll.push({
      ['$.a.a.a.a' + ':' + stringizer.typeKeys.number]: '0',
      ['$.a.a.a.b.b[0]' + ':' + stringizer.typeKeys.number]: '0',
      ['$.a.a.a.b.b[1].c' + ':' + stringizer.typeKeys.string]: 'd',
      ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
    })
    const found = await foo.findOneById(saved.id)
    expect(saved).toStrictEqual(found)
  })

  it('sets the id correctly on found arrays', async () => {
    const saved = await foo.save([[[[0, { foo: [[[1]]] }]]]])
    cmdRes.hGetAll.push({
      ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number]: '0',
      ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number]: '1',
      ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
    })

    const found = await foo.findOneById(saved.id)
    expect(saved.id).toBeTruthy()
    expect(saved.id).toStrictEqual(found.id)
  })

  it('adds the key prefix to the passed id if not passed', async () => {
    const foundHash = {
      ['$.id' + ':' + stringizer.typeKeys.string]: 'foo__12345'
    }

    cmdRes.hGetAll.push(foundHash)
    const result = await foo.findOneById('12345')
    expect(result.id).toEqual('foo__12345')
    expect(mockClient.hGetAll.mock.calls[0][0]).toEqual('foo__12345')
  })

  it('does not add the key prefix to the passed id if passed', async () => {
    const foundHash = {
      ['$.id' + ':' + stringizer.typeKeys.string]: 'foo__12345'
    }

    cmdRes.hGetAll.push(foundHash)
    const result = await foo.findOneById('foo__12345')
    expect(result.id).toEqual('foo__12345')
    expect(mockClient.hGetAll.mock.calls[0][0]).toEqual('foo__12345')
  })
})

describe('deleteById', () => {
  it('calls del for the id', async () => {
    await foo.deleteById('foo__123')
    expect(mockMulti.del.mock.calls[0]).toEqual(['foo__123'])
  })

  it('adds the key prefix to the passed id if not passed', async () => {
    await foo.deleteById('123')
    expect(mockMulti.del.mock.calls[0]).toEqual(['foo__123'])
  })

  it('calls zRem if a collection key is provided', async () => {
    await foo.deleteById('123')
    expect(mockMulti.zRem.mock.calls[0]).toEqual(['foo', 'foo__123'])
  })
})

describe('findAll', () => {
  it('calls zRange with the correct start and end indexes', async () => {
    await foo.findAll()
    expect(mockClient.zRange.mock.calls[0].slice(0, -1)).toEqual(['foo', 0, 9])
  })

  it('uses the correct range for user supplied ranges', async () => {
    await foo.findAll(3, 25)
    expect(mockClient.zRange.mock.calls[0].slice(0, -1)).toEqual(['foo', 75, 99])
  })

  it('returns an empty array if no ids are found', async () => {
    cmdRes.zRange.push(undefined)
    const result = await foo.findAll()
    expect(result).toEqual([])
  })

  it('calls HGETALL for each id found', async () => {
    cmdRes.zRange.push(['foo__1', 'foo__2', 'foo__3', 'foo__4'])
    await foo.findAll()

    expect(mockClient.hGetAll.mock.calls.map(a => a[0])).toEqual(['foo__1', 'foo__2', 'foo__3', 'foo__4'])
  })
})
