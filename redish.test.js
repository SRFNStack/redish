const redish = require( './src/index.js' )
const ObjectID = require( 'isomorphic-mongo-objectid' )
const stringizer = require( './src/stringizer.js' )

const cmdRes = {
    HDEL: [ 'ok' ],
    HKEYS: [ [] ],
    HMSET: [ 'ok' ],
    HMGET: [ {} ],
    MULTI: [ 'ok' ],
    ZADD: [ 'ok' ],
    EXEC: [ 'ok' ],
    HGETALL: []
}

const mockClient = {
    send_command: jest.fn( ( cmd, ...args ) => {
        let cb = args.slice( -1 )[ 0 ]
        if( cmdRes[ cmd ] ) {
            if( cmdRes[ cmd ].length > 1 ) {
                cb( undefined, cmdRes[ cmd ].pop() )
            } else {
                cb( undefined, cmdRes[ cmd ][ 0 ] )
            }
        } else {
            cb( undefined, [] )
        }
    } )
}

const testCollection = redish.collection( mockClient, 'test' )

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

const cmdArgs = cmd => mockClient.send_command.mock.calls.find( args => args[ 0 ] === cmd )

afterEach( () => {
    jest.clearAllMocks()
} )

describe( 'save', () => {

    it( 'can only save truthy objects', async() => {
        for( let badValue of [ null, undefined, false, '', 5, -10, NaN ] ) {
            await expect( testCollection.save( badValue ) ).rejects.toThrow( 'You can only save truthy objects with redish' )
        }
    } )

    it( 'does not overwrite ids if set', async() => {
        await testCollection.save( { id: 'unique' } )
        expect( cmdArgs( 'HMSET' )[ 1 ] ).toStrictEqual( [ 'unique', '$.id' + ':' + stringizer.typeKeys.string, 'unique' ] )
    } )

    it( 'generates an object id hex string if id is not set', async() => {
        let result = await testCollection.save( {} )
        expect( ObjectID( result.id ).toString() ).toBe( result.id )
        expect( cmdArgs( 'HMSET' )[ 1 ] ).toStrictEqual( [ result.id, '$.id' + ':' + stringizer.typeKeys.string, result.id ] )
    } )

    it( 'sends hdel command when keys are deleted from an existing object', async() => {
        cmdRes.HKEYS.push( [ '$.id' + ':' + stringizer.typeKeys.string, '$.foo' ] )
        await testCollection.save( { id: 'id' } )
        expect( cmdArgs( 'HDEL' )[ 1 ] ).toStrictEqual( [ 'id', '$.foo' ] )
    } )

    it( 'watches the keys if it needs to delete fields to ensure consistent updates', async() => {
        cmdRes.HKEYS.push( [ '$.id' + ':' + stringizer.typeKeys.string, '$.foo' ] )
        await testCollection.save( { id: 'id' } )
        expect( cmdArgs( 'WATCH' )[ 1 ] ).toStrictEqual( [ 'id' ] )
    } )

    it( 'does not send hkeys or hdel commands if the object is new', async() => {
        await testCollection.save( {} )
        expect( cmdArgs( 'HKEYS' ) ).toStrictEqual( undefined )
        expect( cmdArgs( 'HDEL' ) ).toStrictEqual( undefined )
    } )

    it( 'does not send hdel command if no keys were deleted', async() => {
        cmdRes.HKEYS.push( [ '$.id' + ':' + stringizer.typeKeys.string, '$.foo' + ':' + stringizer.typeKeys.string ] )
        await testCollection.save( { id: 'id', foo: 'foo' } )
        expect( cmdArgs( 'HDEL' ) ).toStrictEqual( undefined )
    } )

    it( 'adds the objects id to the collection\'s zset with a score of 0 if it\'s a new object', async() => {
        let result = await testCollection.save( {} )
        expect( cmdArgs( 'ZADD' )[ 1 ] ).toStrictEqual( [ 'test', result.id, 0 ] )
    } )

    it( 'saves array root objects correctly', async() => {
        let result = await testCollection.save( [ 5, 's' ] )
        expect( ObjectID( result.id ).toString() ).toBe( result.id )
        expect( cmdArgs( 'HMSET' )[ 1 ] )
            .toStrictEqual( [
                                result.id,
                                '$[0]' + ':' + stringizer.typeKeys.number, '5',
                                '$[1]' + ':' + stringizer.typeKeys.string, 's',
                                '$.id' + ':' + stringizer.typeKeys.string, result.id
                            ] )
    } )

    it( 'serializes types correctly', async() => {

        let result = await testCollection.save( { ...allTypes } )
        expect( cmdArgs( 'HMSET' )[ 1 ] )
            .toStrictEqual( [
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
        let result = await testCollection.save( [ [ [ [ 0, { foo: [ [ [ 1 ] ] ] } ] ] ] ] )
        expect( cmdArgs( 'HMSET' )[ 1 ] )
            .toStrictEqual( [
                                result.id,
                                '$[0][0][0][0]' + ':' + stringizer.typeKeys.number, '0',
                                '$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number, '1',
                                '$.id' + ':' + stringizer.typeKeys.string, result.id
                            ] )

    } )

    it( 'serializes nested objects correctly', async() => {

        let result = await testCollection.save( { a: { a: { a: { a: 0, b: { b: [ 0, { c: 'd' } ] } } } } } )
        expect( cmdArgs( 'HMSET' )[ 1 ] )
            .toStrictEqual( [
                                result.id,
                                '$.a.a.a.a' + ':' + stringizer.typeKeys.number, '0',
                                '$.a.a.a.b.b[0]' + ':' + stringizer.typeKeys.number, '0',
                                '$.a.a.a.b.b[1].c' + ':' + stringizer.typeKeys.string, 'd',
                                '$.id' + ':' + stringizer.typeKeys.string, result.id
                            ] )
    } )


    it( 'performs everything in a single transaction', async() => {
        let result = await testCollection.save( {} )

        let calls = mockClient.send_command.mock.calls
        expect( calls.filter( a => a[ 0 ] === 'WATCH' ).length ).toStrictEqual( 1 )
        expect( calls.filter( a => a[ 0 ] === 'MULTI' ).length ).toStrictEqual( 1 )
        expect( calls.filter( a => a[ 0 ] === 'EXEC' ).length ).toStrictEqual( 1 )

        let watchI, multiI, execI

        calls.forEach( ( args, i ) => {
            if( args[ 0 ] === 'WATCH' ) watchI = i
            if( args[ 0 ] === 'MULTI' ) multiI = i
            if( args[ 0 ] === 'EXEC' ) execI = i
        } )

        expect( watchI < multiI < execI ).toStrictEqual( true )
    } )

    it( 'throws an error if the transaction was unsuccessful', async() => {
        cmdRes.EXEC.push( null )
        expect( testCollection.save( {} ) ).rejects.toThrow()
    } )

} )


describe( 'findOneById', () => {
    it( 'should require a truthy id is passed', async() => {
        for( let badValue of [ null, undefined, false, '', 0, NaN ] ) {
            await expect( testCollection.findOneById( badValue ) ).rejects.toThrow( 'You must provide an id' )
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

        cmdRes.HGETALL.push( foundHash )
        let result = await testCollection.findOneById( id )

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
        let saved = await testCollection.save( [ [ [ [ 0, { foo: [ [ [ 1 ] ] ] } ] ] ] ] )
        cmdRes.HGETALL.push({
                                ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number]: '0',
                                ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number]: '1',
                                ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
                            })

        let found = await testCollection.findOneById( saved.id )
        expect( saved ).toStrictEqual( found )
    } )


    it( 'deserializes nested objects correctly', async() => {
        let saved = await testCollection.save( { a: { a: { a: { a: 0, b: { b: [ 0, { c: 'd' } ] } } } } } )
        cmdRes.HGETALL.push( {
                                 [ '$.a.a.a.a' + ':' + stringizer.typeKeys.number ]: '0',
                                 [ '$.a.a.a.b.b[0]' + ':' + stringizer.typeKeys.number ]: '0',
                                 [ '$.a.a.a.b.b[1].c' + ':' + stringizer.typeKeys.string ]: 'd',
                                 [ '$.id' + ':' + stringizer.typeKeys.string ]: saved.id
                             } )
        let found = await testCollection.findOneById( saved.id )
        expect( saved ).toStrictEqual( found )
    } )

    it( 'sets the id correctly on found arrays', async() => {
        let saved = await testCollection.save( [ [ [ [ 0, { foo: [ [ [ 1 ] ] ] } ] ] ] ] )
        cmdRes.HGETALL.push({
                                ['$[0][0][0][0]' + ':' + stringizer.typeKeys.number]: '0',
                                ['$[0][0][0][1].foo[0][0][0]' + ':' + stringizer.typeKeys.number]: '1',
                                ['$.id' + ':' + stringizer.typeKeys.string]: saved.id
                            })

        let found = await testCollection.findOneById( saved.id )
        expect(saved.id).toBeTruthy()
        expect( saved.id ).toStrictEqual( found.id )

    } )

} )