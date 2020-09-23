const redish = require( './src/index.js' )
const ObjectID = require( 'isomorphic-mongo-objectid' )
const redis = require( 'redis' )
const client = redis.createClient( 16379 )
// client.auth("90d959b7-03b1-43f7-8f55-8ea716a29b2f", console.log)
const db = redish.createDb( client )

afterAll( () => client.quit() )
describe(
    'redish',
    () => {
        it( 'should save and retrieve complex objects containing all primitive objects correctly', async() => {

            let orig = {
                emptyObject: {},
                emptyArray: [],
                emptyString: '',
                null: null,
                undefined: undefined,
                boolean: true,
                string: 'str',
                BigInt: BigInt( '420420420420420420' ),
                symbol: Symbol.for( 'foo' ),
                function: ( arg ) => console.log( 'hello ', arg ),
                number: 1,
                date: new Date(),
                nestedArrays: [ 0, [ 0, [ 0, { a: [ [ [ 0 ] ] ] } ] ] ],
                nestedObjects: {
                    a: 5,
                    b: {
                        a: 5,
                        b: {
                            a: 5,
                            b: 6,
                            c: [ 0, [ [ [ 1 ] ] ] ]
                        }
                    }
                }
            }
            let saved = await db.save( orig )
            let found = await db.findOneById( saved.id )
            expect( ObjectID( saved.id ).toString() ).toBe( saved.id )
            expect( saved.id ).toBe( found.id )
            for( let result of [ saved, found ] ) {
                expect( result.emptyObject ).toStrictEqual( orig.emptyObject )
                expect( result.emptyArray ).toStrictEqual( orig.emptyArray )
                expect( result.emptyString ).toStrictEqual( orig.emptyString )
                expect( result.null ).toStrictEqual( orig.null )
                expect( result.undefined ).toStrictEqual( orig.undefined )
                expect( result.boolean ).toStrictEqual( orig.boolean )
                expect( result.string ).toStrictEqual( orig.string )
                expect( result.BigInt ).toStrictEqual( orig.BigInt )
                expect( result.symbol ).toStrictEqual( orig.symbol )
                expect( result.function.toString ).toStrictEqual( orig.function.toString )
                expect( result.number ).toStrictEqual( orig.number )
                expect( result.date ).toStrictEqual( orig.date )
                expect( result.nestedArrays[ 0 ] ).toStrictEqual( result.nestedArrays[ 0 ] )
                expect( result.nestedArrays[ 1 ][ 0 ] ).toStrictEqual( result.nestedArrays[ 1 ][ 0 ] )
                expect( result.nestedArrays[ 1 ][ 1 ][ 0 ] ).toStrictEqual( result.nestedArrays[ 1 ][ 1 ][ 0 ] )
                expect( result.nestedArrays[ 1 ][ 1 ][ 1 ].a[ 0 ][ 0 ][ 0 ] ).toStrictEqual( result.nestedArrays[ 1 ][ 1 ][ 1 ].a[ 0 ][ 0 ][ 0 ] )
                expect( result.nestedObjects.a ).toStrictEqual( orig.nestedObjects.a )
                expect( result.nestedObjects.b.a ).toStrictEqual( orig.nestedObjects.b.a )
                expect( result.nestedObjects.b.b.a ).toStrictEqual( orig.nestedObjects.b.b.a )
                expect( result.nestedObjects.b.b.b ).toStrictEqual( orig.nestedObjects.b.b.b )
                expect( result.nestedObjects.b.b.c[ 0 ] ).toStrictEqual( orig.nestedObjects.b.b.c[ 0 ] )
                expect( result.nestedObjects.b.b.c[ 1 ][ 0 ][ 0 ][ 0 ] ).toStrictEqual( orig.nestedObjects.b.b.c[ 1 ][ 0 ][ 0 ][ 0 ] )
            }


        } )


        it( 'should delete keys that are deleted from objects', async() => {

            let update = await db.save( { keep: 'foo', del: 'bar' } )
            delete update.del
            update.add = 'boop'

            let updated = await db.save( update )

            let updateFound = await db.findOneById( updated.id )
            expect( updateFound.id ).toBe( update.id )
            expect( updateFound.del ).toBe( undefined )
            expect( updateFound.add ).toBe( 'boop' )
            expect( updateFound.keep ).toBe( 'foo' )

        } )

        it( 'should not delete keys that are deleted from objects when using upsert', async() => {

            let update = await db.upsert( { keep: 'foo', del: 'bar' } )
            delete update.del
            update.add = 'boop'

            let updated = await db.upsert( update )

            let updateFound = await db.findOneById( updated.id )
            expect( updateFound.id ).toBe( update.id )
            expect( updateFound.del ).toBe( 'bar' )
            expect( updateFound.add ).toBe( 'boop' )
            expect( updateFound.keep ).toBe( 'foo' )

        } )

        it( 'should save and retrieve arrays correctly', async() => {
            let array = await db.save( [ 1, 2, { foo: 'bar' } ] )
            let foundArray = await db.findOneById( array.id )
            expect( foundArray.id ).toBe( array.id )
            expect( foundArray[ 0 ] ).toBe( array[ 0 ] )
            expect( foundArray[ 1 ] ).toBe( array[ 1 ] )
            expect( foundArray[ 2 ].foo ).toBe( array[ 2 ].foo )
        } )

        it('should be able to find all of the items in a collection', async()=>{
            let collectionKey = 'k' + new Date().getTime()
            let saved = await Promise.all( [1,2,3,4,5,6,7,8,9,10].map( k=>db.save( {k}, {collectionKey: collectionKey})))
            let found = await db.findAll(collectionKey)
            expect(found).toStrictEqual(saved)

        })

        it('should be able to delete records correctly', async()=>{
            let saved = await db.save({yep:true})
            let beforeDelete = await db.findOneById(saved.id)
            expect(saved).toStrictEqual(beforeDelete)
            await db.deleteById(saved.id)
            let found = await db.findOneById(saved.id)
            expect(found).toStrictEqual(null)
        })

    }
)