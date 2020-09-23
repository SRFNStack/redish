const redish = require( './src/index.js' )
const ObjectID = require( 'isomorphic-mongo-objectid' )
const stringizer = require( './src/stringizer.js' )


const cmdRes = {
    hdel: [ 'ok' ],
    hkeys: [ [] ],
    hmset: [ 'ok' ],
    hmget: [ {} ],
    multi: [ 'ok' ],
    zadd: [ 'ok' ],
    exec: [ 'ok' ],
    hgetall: [],
    zrange: []
}
const getResponse = (cmd) => {
    if( cmdRes[ cmd ] ) {
        if( cmdRes[ cmd ].length > 1 ) {
            return cmdRes[ cmd ].pop()
        } else {
            return cmdRes[ cmd ][ 0 ]
        }
    } else {
        return null
    }
}
let mockMulti = {
    exec: jest.fn((cb)=> cb()),
    hmset: jest.fn(),
    hdel: jest.fn(),
    zadd: jest.fn(),
    del: jest.fn(),
    zrem: jest.fn(),
}


const mockClient = {
    watch: jest.fn((id, cb)=> cb()),
    hkeys: jest.fn((id, cb)=>{
        cb(undefined, getResponse('hkeys'))
    }),
    hgetall: jest.fn((key, cb)=>{
        cb(undefined, getResponse('hgetall'))
    }),
    zrange: jest.fn((key, start, end, cb)=>{
        cb(undefined, getResponse('zrange'))
    }),
    multi: jest.fn( () => mockMulti )
}

const db = redish.createDb( mockClient )

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

    it( 'can only save truthy objects', async() => {
        for( let badValue of [ null, undefined, false, '', 5, -10, NaN ] ) {
            await expect( db.save( badValue ) ).rejects.toThrow( 'You can only save truthy objects with redish' )
        }
    } )

    it( 'does not overwrite ids if set', async() => {
        await db.save( { id: 'unique'}, {audit: false} )
        expect( mockMulti.hmset.mock.calls[0] ).toEqual( ['unique', '$.id' + ':' + stringizer.typeKeys.string, 'unique'] )
    } )

    it( 'generates an object id hex string if id is not set', async() => {
        let result = await db.save( {}, {audit: false} )
        expect( ObjectID( result.id ).toString() ).toBe( result.id )
        expect( mockMulti.hmset.mock.calls[0]).toEqual( [result.id, '$.id' + ':' + stringizer.typeKeys.string, result.id] )
    } )

    it( 'sends hdel command when keys are deleted from an existing object', async() => {
        cmdRes.hkeys.push( [ '$.id' + ':' + stringizer.typeKeys.string, '$.foo' ] )
        await db.save( { id: 'id' }, {audit: false} )
        expect( mockMulti.hdel.mock.calls[0]).toEqual( [ 'id', '$.foo' ] )
    } )

    it( 'doesn\'t send hdel command when using upsert to update an existing object', async() => {
        cmdRes.hkeys.push( [ '$.id' + ':' + stringizer.typeKeys.string, '$.foo' ] )
        await db.upsert( { id: 'id' }, {audit: false} )
        expect( mockMulti.hdel.mock.calls.length).toEqual( 0 )
    } )

    it( 'watches the keys if it needs to delete fields to ensure consistent updates', async() => {
        cmdRes.hkeys.push( [ '$.id' + ':' + stringizer.typeKeys.string, '$.foo' ] )
        await db.save( { id: 'id' } )
        expect(mockClient.watch.mock.calls[0][0]).toEqual( 'id' )
    } )

    it( 'does not send hkeys or hdel commands if the object is new', async() => {
        await db.save( {} )
        expect( mockClient.hkeys.mock.calls.length ).toStrictEqual( 0 )
        expect(  mockMulti.hdel.mock.calls.length ).toStrictEqual( 0 )
    } )

    it( 'does not send hdel command if no keys were deleted', async() => {
        cmdRes.hkeys.push( [ '$.id' + ':' + stringizer.typeKeys.string, '$.foo' + ':' + stringizer.typeKeys.string ] )
        await db.save( { id: 'id', foo: 'foo' } )
        expect(  mockMulti.hdel.mock.calls.length ).toStrictEqual( 0 )
    } )

    it( 'adds the objects id to the collection\'s zset with a score of 0 if it\'s a new object', async() => {
        let result = await db.save( {}, {collectionKey: 'test'} )
        expect(mockMulti.zadd.mock.calls[0] ).toEqual( [ 'test', 0, result.id ] )
    } )

    it( 'does not add the object\'s id to the createDb\'s zset if no collection key is provided', async() => {
        let result = await db.save( {} )
        expect( mockMulti.zadd.mock.calls.length ).toStrictEqual( 0 )
    } )

    it( 'saves array root objects correctly', async() => {
        let result = await db.save( [ 5, 's' ], {audit: false} )
        expect( ObjectID( result.id ).toString() ).toBe( result.id )
        expect( mockMulti.hmset.mock.calls[0] )
            .toEqual( [
                                result.id,
                                '$[0]' + ':' + stringizer.typeKeys.number, '5',
                                '$[1]' + ':' + stringizer.typeKeys.string, 's',
                                '$.id' + ':' + stringizer.typeKeys.string, result.id
                            ] )
    } )

    it( 'serializes types correctly', async() => {

        let result = await db.save( { ...allTypes }, {audit: false} )
        expect( mockMulti.hmset.mock.calls[0]  )
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

    it( 'serializes nested arrays correctly', async() => {
        let result = await db.save( [ [ [ [ 0, { foo: [ [ [ 1 ] ] ] } ] ] ] ], {audit: false} )
        expect( mockMulti.hmset.mock.calls[0]  )
            .toEqual( [
                                result.id,
                                '$[0][0][0][0]' + ':' + stringizer.typeKeys.number, '0',
                                '$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number, '1',
                                '$.id' + ':' + stringizer.typeKeys.string, result.id
                            ] )

    } )

    it( 'serializes nested objects correctly', async() => {

        let result = await db.save( { a: { a: { a: { a: 0, b: { b: [ 0, { c: 'd' } ] } } } } }, {audit: false} )
        expect( mockMulti.hmset.mock.calls[0]  )
            .toEqual( [
                                result.id,
                                '$.a.a.a.a' + ':' + stringizer.typeKeys.number, '0',
                                '$.a.a.a.b.b[0]' + ':' + stringizer.typeKeys.number, '0',
                                '$.a.a.a.b.b[1].c' + ':' + stringizer.typeKeys.string, 'd',
                                '$.id' + ':' + stringizer.typeKeys.string, result.id
                            ] )
    } )

    it('sets the audit fields on new objects correctly', async ()=> {
        let result = await db.save( {} , {auditUser: 'me'})
        expect(new Date(result.createdAt).getTime() > 0).toStrictEqual(true)
        expect(result.createdBy).toStrictEqual('me')
        expect(result.updatedAt).toStrictEqual(undefined)
        expect(result.updatedBy).toStrictEqual(undefined)
    })

    it('sets the audit fields on existing objects correctly', async ()=> {
        let theDate = (new Date().getTime() - 100000)
        let result = await db.save( {id: 1234, createdAt: theDate, createdBy: 'me'} , {auditUser: 'me'})
        expect(result.createdAt).toStrictEqual(theDate)
        expect(result.createdBy).toStrictEqual('me')
        expect(new Date(result.updatedAt).getTime() > 0).toStrictEqual(true)
        expect(result.updatedBy).toStrictEqual('me')
    })

} )


describe( 'findOneById', () => {
    it( 'should require a truthy id is passed', async() => {
        for( let badValue of [ null, undefined, false, '', 0, NaN ] ) {
            await expect( db.findOneById( badValue ) ).rejects.toThrow( 'You must provide an id' )
        }
    } )

    it( 'deserializes types correctly', async() => {
        let id = ObjectID().toString()
        let origDate = allTypes.Date
        let foundHash = {
            [ '$.emptyObject' + ':' + stringizer.typeKeys.emptyObject ]: '{}',
            [ '$.emptyArray' + ':' + stringizer.typeKeys.emptyArray ]: '[]',
            [ '$.emptyString' + ':' + stringizer.typeKeys.emptyString ]: '\'\'',
            [ '$.null' + ':' + stringizer.typeKeys.null ]: 'null',
            [ '$.undefined' + ':' + stringizer.typeKeys.undefined ]: 'undefined',
            [ '$.boolean' + ':' + stringizer.typeKeys.boolean ]: 'true',
            [ '$.string' + ':' + stringizer.typeKeys.string ]: 'string',
            [ '$.BigInt' + ':' + stringizer.typeKeys.BigInt ]: '123456789123456789',
            [ '$.Symbol' + ':' + stringizer.typeKeys.Symbol ]: 'Symbol(symbol)',
            [ '$.function' + ':' + stringizer.typeKeys.function ]: 'arg => console.log(arg)',
            [ '$.number' + ':' + stringizer.typeKeys.number ]: '100',
            [ '$.Date' + ':' + stringizer.typeKeys.Date ]: origDate.toISOString(),
            [ '$.id' + ':' + stringizer.typeKeys.string ]: id
        }

        cmdRes.hgetall.push( foundHash )
        let result = await db.findOneById( id )

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

    it( 'deserializes nested arrays correctly', async() => {
        let saved = await db.save( [ [ [ [ 0, { foo: [ [ [ 1 ] ] ] } ] ] ] ], {audit: false} )
        cmdRes.hgetall.push({
                                ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number]: '0',
                                ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number]: '1',
                                ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
                            })

        let found = await db.findOneById( saved.id )
        expect( saved ).toStrictEqual( found )
    } )


    it( 'deserializes nested objects correctly', async() => {
        let saved = await db.save( { a: { a: { a: { a: 0, b: { b: [ 0, { c: 'd' } ] } } } } }, {audit: false} )
        cmdRes.hgetall.push( {
                                 [ '$.a.a.a.a' + ':' + stringizer.typeKeys.number ]: '0',
                                 [ '$.a.a.a.b.b[0]' + ':' + stringizer.typeKeys.number ]: '0',
                                 [ '$.a.a.a.b.b[1].c' + ':' + stringizer.typeKeys.string ]: 'd',
                                 [ '$.id' + ':' + stringizer.typeKeys.string ]: saved.id
                             } )
        let found = await db.findOneById( saved.id )
        expect( saved ).toStrictEqual( found )
    } )

    it( 'sets the id correctly on found arrays', async() => {
        let saved = await db.save( [ [ [ [ 0, { foo: [ [ [ 1 ] ] ] } ] ] ] ] )
        cmdRes.hgetall.push({
                                ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number]: '0',
                                ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number]: '1',
                                ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
                            })

        let found = await db.findOneById( saved.id )
        expect(saved.id).toBeTruthy()
        expect( saved.id ).toStrictEqual( found.id )

    } )

} )

describe("deleteById", ()=>{

    it("calls del for the id", async() => {
        let result = await db.deleteById(123)
        expect(mockMulti.del.mock.calls[0]).toEqual([123])
    })

    it("calls zrem if a collection key is provided", async() => {
        let result = await db.deleteById(123, "key")
        expect(mockMulti.zrem.mock.calls[0]).toEqual(['key', 123])
    })

    it("doesn't call zrem if no collection key is provided", async() => {
        let result = await db.deleteById(123)
        expect(mockMulti.zrem.mock.calls.length).toStrictEqual(0)
    })
})

describe("findAll", ()=> {
    it("calls zrange with the correct start and end indexes", async() =>{
        let result = await db.findAll('key')
        expect(mockClient.zrange.mock.calls[0].slice(0,-1)).toEqual(['key', 0, 9])
    })

    it("uses the correct range for user supplied ranges", async() =>{
        let result = await db.findAll('key', 3, 25)
        expect(mockClient.zrange.mock.calls[0].slice(0,-1)).toEqual(['key', 75, 99])
    })

    it("returns an empty array if no ids are found", async()=>{
        cmdRes.zrange.push(undefined)
        let result = await db.findAll('key')
        expect(result).toEqual([])
    })

    it("calls HGETALL for each id found", async()=>{
        cmdRes.zrange.push([1,2,3,4])
        let result = await db.findAll('key')

        expect(mockClient.hgetall.mock.calls.map(a=>a[0])).toEqual([1,2,3,4])
    })
})