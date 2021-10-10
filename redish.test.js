const redish = require( './src/index.js' )
const ObjectID = require( 'isomorphic-mongo-objectid' )
const stringizer = require( './src/stringizer.js' )
const Ajv = require( "ajv" );


const cmdRes = {
    hdel: ['ok'],
    hkeys: [[]],
    hmset: ['ok'],
    hmget: [{}],
    multi: ['ok'],
    zadd: ['ok'],
    exec: ['ok'],
    hgetall: [],
    zrange: []
}
const getResponse = ( cmd ) => {
    if( cmdRes[cmd] ) {
        if( cmdRes[cmd].length > 1 ) {
            return cmdRes[cmd].pop()
        } else {
            return cmdRes[cmd][0]
        }
    } else {
        return null
    }
}
let mockMulti = {
    exec: jest.fn( ( cb ) => cb() ),
    hmset: jest.fn(),
    hdel: jest.fn(),
    zadd: jest.fn(),
    del: jest.fn(),
    zrem: jest.fn(),
}


const mockClient = {
    watch: jest.fn( ( id, cb ) => cb() ),
    hkeys: jest.fn( ( id, cb ) => {
        cb( undefined, getResponse( 'hkeys' ) )
    } ),
    hgetall: jest.fn( ( key, cb ) => {
        cb( undefined, getResponse( 'hgetall' ) )
    } ),
    zrange: jest.fn( ( key, start, end, cb ) => {
        cb( undefined, getResponse( 'zrange' ) )
    } ),
    multi: jest.fn( () => mockMulti )
}

const db = redish.createDb( mockClient )

const foo = db.collection( 'foo' )
const audit = db.collection( 'audit', { enableAudit: true } )
const scheme = db.collection( 'scheme', {
    schema: {
        type: 'object',
        properties: {
            name: { type: 'string' },
            favoriteColor: { type: 'string' }
        },
        required: ['name']
    }
} )
const allTypes = {
    emptyObject: {},
    emptyArray: [],
    emptyString: '',
    null: null,
    undefined: undefined,
    boolean: true,
    string: 'string',
    BigInt: BigInt( '123456789123456789' ),
    Symbol: Symbol.for( 'symbol' ),
    function: ( arg ) => console.log( arg ),
    number: 100,
    Date: new Date()
}


afterEach( () => {
    jest.clearAllMocks()
} )

describe( 'save', () => {

    it( 'can only save truthy objects', async () => {
        for( let badValue of [null, undefined, false, '', 5, -10, NaN] ) {
            await expect( foo.save( badValue ) ).rejects.toThrow( 'You can only save truthy objects with redish' )
        }
    } )

    it( 'prefixes the id with the collection if set', async () => {
        await foo.save( { id: 'unique' } )
        expect( mockMulti.hmset.mock.calls[0] ).toEqual( ['foo__unique', '$.id' + ':' + stringizer.typeKeys.string, 'foo__unique'] )
    } )

    it( 'generates an object id hex string if id is not set', async () => {
        let result = await foo.save( {} )
        let objectId = result.id.split( 'foo__' )[1];
        expect( ObjectID( objectId ).toString() ).toBe( objectId )
        expect( mockMulti.hmset.mock.calls[0] ).toEqual( [result.id, '$.id' + ':' + stringizer.typeKeys.string, result.id] )
    } )

    it( 'sends hdel command when keys are deleted from an existing object', async () => {
        cmdRes.hkeys.push( ['$.id' + ':' + stringizer.typeKeys.string, '$.foo'] )
        await foo.save( { id: 'id' } )
        expect( mockMulti.hdel.mock.calls[0] ).toEqual( ['foo__id', '$.foo'] )
    } )

    it( 'doesn\'t send hdel command when using upsert to update an existing object', async () => {
        cmdRes.hkeys.push( ['$.id' + ':' + stringizer.typeKeys.string, '$.foo'] )
        await foo.upsert( { id: 'id' } )
        expect( mockMulti.hdel.mock.calls.length ).toEqual( 0 )
    } )

    it( 'watches the keys if it needs to delete fields to ensure consistent updates', async () => {
        cmdRes.hkeys.push( ['$.id' + ':' + stringizer.typeKeys.string, '$.foo'] )
        await foo.save( { id: 'id' } )
        expect( mockClient.watch.mock.calls[0][0] ).toEqual( ['foo__id', 'foo'] )
    } )

    it( 'does not send hkeys or hdel commands if the object is new', async () => {
        await foo.save( {} )
        expect( mockClient.hkeys.mock.calls.length ).toStrictEqual( 0 )
        expect( mockMulti.hdel.mock.calls.length ).toStrictEqual( 0 )
    } )

    it( 'does not send hdel command if no keys were deleted', async () => {
        cmdRes.hkeys.push( ['$.id' + ':' + stringizer.typeKeys.string, '$.foo' + ':' + stringizer.typeKeys.string] )
        await foo.save( { id: 'id', foo: 'foo' } )
        expect( mockMulti.hdel.mock.calls.length ).toStrictEqual( 0 )
    } )

    it( 'adds the objects id to the collection\'s zset with a score of 0 if it\'s a new object', async () => {
        let result = await foo.save( {} )
        expect( mockMulti.zadd.mock.calls[0] ).toEqual( ['foo', 0, result.id] )
    } )

    it( 'saves array root objects correctly', async () => {
        let result = await foo.save( [5, 's'] )
        let objectId = result.id.split( 'foo__' )[1];
        expect( ObjectID( objectId ).toString() ).toBe( objectId )
        expect( mockMulti.hmset.mock.calls[0] )
            .toEqual( [
                result.id,
                '$[0]' + ':' + stringizer.typeKeys.number, '5',
                '$[1]' + ':' + stringizer.typeKeys.string, 's',
                '$.id' + ':' + stringizer.typeKeys.string, result.id
            ] )
    } )

    it( 'serializes types correctly', async () => {

        let result = await foo.save( { ...allTypes } )
        expect( mockMulti.hmset.mock.calls[0] )
            .toEqual( [
                result.id,
                '$.emptyObject' + ':' + stringizer.typeKeys.emptyObject, '{}',
                '$.emptyArray' + ':' + stringizer.typeKeys.emptyArray, '[]',
                '$.emptyString' + ':' + stringizer.typeKeys.emptyString, '\'\'',
                '$.null' + ':' + stringizer.typeKeys.null, 'null',
                '$.undefined' + ':' + stringizer.typeKeys.undefined, 'undefined',
                '$.boolean' + ':' + stringizer.typeKeys.boolean, 'true',
                '$.string' + ':' + stringizer.typeKeys.string, 'string',
                '$.BigInt' + ':' + stringizer.typeKeys.BigInt, '123456789123456789',
                '$.Symbol' + ':' + stringizer.typeKeys.Symbol, 'Symbol(symbol)',
                '$.function' + ':' + stringizer.typeKeys.function, 'arg => console.log(arg)',
                '$.number' + ':' + stringizer.typeKeys.number, '100',
                '$.Date' + ':' + stringizer.typeKeys.Date, allTypes.Date.toISOString(),
                '$.id' + ':' + stringizer.typeKeys.string, result.id
            ] )

    } )

    it( 'serializes nested arrays correctly', async () => {
        let result = await foo.save( [[[[0, { foo: [[[1]]] }]]]] )
        expect( mockMulti.hmset.mock.calls[0] )
            .toEqual( [
                result.id,
                '$[0][0][0][0]' + ':' + stringizer.typeKeys.number, '0',
                '$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number, '1',
                '$.id' + ':' + stringizer.typeKeys.string, result.id
            ] )

    } )

    it( 'serializes nested objects correctly', async () => {

        let result = await foo.save( { a: { a: { a: { a: 0, b: { b: [0, { c: 'd' }] } } } } } )
        expect( mockMulti.hmset.mock.calls[0] )
            .toEqual( [
                result.id,
                '$.a.a.a.a' + ':' + stringizer.typeKeys.number, '0',
                '$.a.a.a.b.b[0]' + ':' + stringizer.typeKeys.number, '0',
                '$.a.a.a.b.b[1].c' + ':' + stringizer.typeKeys.string, 'd',
                '$.id' + ':' + stringizer.typeKeys.string, result.id
            ] )
    } )

    it( 'sets the audit fields on new objects correctly', async () => {
        let result = await audit.save( {}, 'me' )
        expect( new Date( result.createdAt ).getTime() > 0 ).toStrictEqual( true )
        expect( result.createdBy ).toStrictEqual( 'me' )
        expect( result.updatedAt ).toStrictEqual( undefined )
        expect( result.updatedBy ).toStrictEqual( undefined )
    } )

    it( 'sets the audit fields on existing objects correctly', async () => {
        let theDate = ( new Date().getTime() - 100000 )
        let result = await audit.save( { id: '1234', createdAt: theDate, createdBy: 'me' }, 'it' )
        expect( result.createdAt ).toStrictEqual( theDate )
        expect( result.createdBy ).toStrictEqual( 'me' )
        expect( new Date( result.updatedAt ).getTime() > 0 ).toStrictEqual( true )
        expect( result.updatedBy ).toStrictEqual( 'it' )
    } )

    it( 'validates the object using the provided schema', async () => {
        await scheme.save( { id: 'unique', name: 'taco', favoriteColor: 'green' } )
        expect( mockMulti.hmset.mock.calls[0] ).toEqual( ['scheme__unique', '$.id' + ':' + stringizer.typeKeys.string, 'scheme__unique', "$.name:6", "taco", "$.favoriteColor:6", "green"] )
    } )
    it( 'throws an error with messages if the object isn\'t valid per the schema', async () => {
        try {
            await scheme.save( { id: 'unique', favoriteColor: 'green' } )
            fail('save should\'ve thrown')
        } catch( e ) {
            expect(e.validationErrors).toEqual([{
                "instancePath": "",
                "schemaPath": "#/required",
                "keyword": "required",
                "params": {
                    "missingProperty": "name"
                },
                "message": "must have required property 'name'"
            }])
        }

    } )

    it( 'uses ajvOptions if passed', async () => {
        const ajvOptions = db.collection('ajvOptions', {
            ajvOptions: {strictNumbers: true},
            schema: {
                type: 'object',
                properties: {
                    number: { type: 'number' }
                },
                required: ['number']
            }
        })
        try {
            await ajvOptions.save( { id: 'unique', number: '1234' } )
            fail('save should\'ve thrown')
        } catch( e ) {
            expect(e.validationErrors).toEqual([{
                "instancePath": "/number",
                "schemaPath": "#/properties/number/type",
                "keyword": "type",
                "params": {
                    "type": "number"
                },
                "message": "must be number"
            }])
        }

    } )

    it( 'uses ajv if passed', async () => {
        const ajvInst = db.collection('ajv', {
            ajv: new Ajv(
                {strictNumbers: true}
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
            await ajvInst.save( { id: 'unique', number: '1234' } )
            fail('save should\'ve thrown')
        } catch( e ) {
            expect(e.validationErrors).toEqual([{
                "instancePath": "/number",
                "schemaPath": "#/properties/number/type",
                "keyword": "type",
                "params": {
                    "type": "number"
                },
                "message": "must be number"
            }])
        }

    } )
} )


describe( 'findOneById', () => {
    it( 'should require a non empty string id is passed', async () => {
        for( let badValue of [null, undefined, false, '', 0, NaN] ) {
            await expect( foo.findOneById( badValue ) ).rejects.toThrow( 'id must be a non-empty string' )
        }
    } )

    it( 'deserializes types correctly', async () => {
        let id = ObjectID().toString()
        let origDate = allTypes.Date
        let foundHash = {
            ['$.emptyObject' + ':' + stringizer.typeKeys.emptyObject]: '{}',
            ['$.emptyArray' + ':' + stringizer.typeKeys.emptyArray]: '[]',
            ['$.emptyString' + ':' + stringizer.typeKeys.emptyString]: '\'\'',
            ['$.null' + ':' + stringizer.typeKeys.null]: 'null',
            ['$.undefined' + ':' + stringizer.typeKeys.undefined]: 'undefined',
            ['$.boolean' + ':' + stringizer.typeKeys.boolean]: 'true',
            ['$.string' + ':' + stringizer.typeKeys.string]: 'string',
            ['$.BigInt' + ':' + stringizer.typeKeys.BigInt]: '123456789123456789',
            ['$.Symbol' + ':' + stringizer.typeKeys.Symbol]: 'Symbol(symbol)',
            ['$.function' + ':' + stringizer.typeKeys.function]: 'arg => console.log(arg)',
            ['$.number' + ':' + stringizer.typeKeys.number]: '100',
            ['$.Date' + ':' + stringizer.typeKeys.Date]: origDate.toISOString(),
            ['$.id' + ':' + stringizer.typeKeys.string]: id
        }

        cmdRes.hgetall.push( foundHash )
        let result = await foo.findOneById( id )

        expect( result.emptyObject ).toStrictEqual( {} )
        expect( result.emptyArray ).toStrictEqual( [] )
        expect( result.emptyString ).toStrictEqual( '' )
        expect( result.null ).toStrictEqual( null )
        expect( result.undefined ).toStrictEqual( undefined )
        expect( result.boolean ).toStrictEqual( true )
        expect( result.string ).toStrictEqual( 'string' )
        expect( result.BigInt ).toStrictEqual( BigInt( '123456789123456789' ) )
        expect( result.Symbol ).toStrictEqual( Symbol.for( 'symbol' ) )
        expect( result.function.toString() ).toStrictEqual( 'arg => console.log(arg)' )
        expect( result.number ).toStrictEqual( 100 )
        expect( result.Date ).toStrictEqual( origDate )
        expect( result.id ).toStrictEqual( id )
    } )

    it( 'deserializes nested arrays correctly', async () => {
        let saved = await foo.save( [[[[0, { foo: [[[1]]] }]]]], { audit: false } )
        cmdRes.hgetall.push( {
            ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number]: '0',
            ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number]: '1',
            ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
        } )

        let found = await foo.findOneById( saved.id )
        expect( saved ).toStrictEqual( found )
    } )


    it( 'deserializes nested objects correctly', async () => {
        let saved = await foo.save( { a: { a: { a: { a: 0, b: { b: [0, { c: 'd' }] } } } } }, { audit: false } )
        cmdRes.hgetall.push( {
            ['$.a.a.a.a' + ':' + stringizer.typeKeys.number]: '0',
            ['$.a.a.a.b.b[0]' + ':' + stringizer.typeKeys.number]: '0',
            ['$.a.a.a.b.b[1].c' + ':' + stringizer.typeKeys.string]: 'd',
            ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
        } )
        let found = await foo.findOneById( saved.id )
        expect( saved ).toStrictEqual( found )
    } )

    it( 'sets the id correctly on found arrays', async () => {
        let saved = await foo.save( [[[[0, { foo: [[[1]]] }]]]] )
        cmdRes.hgetall.push( {
            ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number]: '0',
            ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number]: '1',
            ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
        } )

        let found = await foo.findOneById( saved.id )
        expect( saved.id ).toBeTruthy()
        expect( saved.id ).toStrictEqual( found.id )

    } )

    it( 'adds the key prefix to the passed id if not passed', async () => {
        let foundHash = {
            ['$.id' + ':' + stringizer.typeKeys.string]: `foo__12345`
        }

        cmdRes.hgetall.push( foundHash )
        let result = await foo.findOneById( '12345' )
        expect(result.id).toEqual('foo__12345')
        expect(mockClient.hgetall.mock.calls[0][0]).toEqual('foo__12345')
    } )

    it( 'does not add the key prefix to the passed id if passed', async () => {
        let foundHash = {
            ['$.id' + ':' + stringizer.typeKeys.string]: `foo__12345`
        }

        cmdRes.hgetall.push( foundHash )
        let result = await foo.findOneById( 'foo__12345' )
        expect(result.id).toEqual('foo__12345')
        expect(mockClient.hgetall.mock.calls[0][0]).toEqual('foo__12345')
    } )

} )

describe( "deleteById", () => {

    it( "calls del for the id", async () => {
        let result = await foo.deleteById( 'foo__123' )
        expect( mockMulti.del.mock.calls[0] ).toEqual( ['foo__123'] )
    } )

    it( 'adds the key prefix to the passed id if not passed', async () => {
        let result = await foo.deleteById( '123' )
        expect( mockMulti.del.mock.calls[0] ).toEqual( ['foo__123'] )
    } )

    it( "calls zrem if a collection key is provided", async () => {
        let result = await foo.deleteById( '123' )
        expect( mockMulti.zrem.mock.calls[0] ).toEqual( ['foo', 'foo__123'] )
    } )

} )

describe( "findAll", () => {
    it( "calls zrange with the correct start and end indexes", async () => {
        let result = await foo.findAll()
        expect( mockClient.zrange.mock.calls[0].slice( 0, -1 ) ).toEqual( ['foo', 0, 9] )
    } )

    it( "uses the correct range for user supplied ranges", async () => {
        let result = await foo.findAll( 3, 25 )
        expect( mockClient.zrange.mock.calls[0].slice( 0, -1 ) ).toEqual( ['foo', 75, 99] )
    } )

    it( "returns an empty array if no ids are found", async () => {
        cmdRes.zrange.push( undefined )
        let result = await foo.findAll( )
        expect( result ).toEqual( [] )
    } )

    it( "calls HGETALL for each id found", async () => {
        cmdRes.zrange.push( ['foo__1', 'foo__2', 'foo__3', 'foo__4'] )
        let result = await foo.findAll( )

        expect( mockClient.hgetall.mock.calls.map( a => a[0] ) ).toEqual( ['foo__1', 'foo__2', 'foo__3', 'foo__4'] )
    } )
} )