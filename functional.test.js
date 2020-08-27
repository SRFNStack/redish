const redish = require( './src/index.js' )
const ObjectID = require( 'isomorphic-mongo-objectid' )
const redis = require( 'redis' )
const client = redis.createClient( 16379 )
// client.auth("90d959b7-03b1-43f7-8f55-8ea716a29b2f", console.log)
const users = redish.collection( client, 'users' )

afterAll(()=>client.quit())
describe( 'redish', () => {

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
                  let saved = await users.save( orig )
                  let found = await users.findOneById( saved.id )
                  expect( ObjectID( saved.id ).toString() ).toBe( saved.id )
                  expect(saved.id).toBe(found.id)
                  for(let result of [saved, found]) {
                      expect(result.emptyObject).toStrictEqual(orig.emptyObject)
                      expect(result.emptyArray).toStrictEqual(orig.emptyArray)
                      expect(result.emptyString).toStrictEqual(orig.emptyString)
                      expect(result.null).toStrictEqual(orig.null)
                      expect(result.undefined).toStrictEqual(orig.undefined)
                      expect(result.boolean).toStrictEqual(orig.boolean)
                      expect(result.string).toStrictEqual(orig.string)
                      expect(result.BigInt).toStrictEqual(orig.BigInt)
                      expect(result.symbol).toStrictEqual(orig.symbol)
                      expect(result.function.toString).toStrictEqual(orig.function.toString)
                      expect(result.number).toStrictEqual(orig.number)
                      expect(result.date).toStrictEqual(orig.date)
                      expect(result.nestedArrays[0]).toStrictEqual(result.nestedArrays[0])
                      expect(result.nestedArrays[1][0]).toStrictEqual(result.nestedArrays[1][0])
                      expect(result.nestedArrays[1][1][0]).toStrictEqual(result.nestedArrays[1][1][0])
                      expect(result.nestedArrays[1][1][1].a[0][0][0]).toStrictEqual(result.nestedArrays[1][1][1].a[0][0][0])
                      expect(result.nestedObjects.a).toStrictEqual(orig.nestedObjects.a)
                      expect(result.nestedObjects.b.a).toStrictEqual(orig.nestedObjects.b.a)
                      expect(result.nestedObjects.b.b.a).toStrictEqual(orig.nestedObjects.b.b.a)
                      expect(result.nestedObjects.b.b.b).toStrictEqual(orig.nestedObjects.b.b.b)
                      expect(result.nestedObjects.b.b.c[0]).toStrictEqual(orig.nestedObjects.b.b.c[0])
                      expect(result.nestedObjects.b.b.c[1][0][0][0]).toStrictEqual(orig.nestedObjects.b.b.c[1][0][0][0])
                  }


              } )


              it( 'should delete keys that are deleted from objects', async() => {

                  let update = await users.save({keep:'foo', del:'bar'})
                  delete update.del
                  update.add = 'boop'

                  let updated = await users.save( update )

                  let updateFound = await users.findOneById( updated.id )
                  expect(updateFound.id).toBe(update.id)
                  expect(updateFound.del).toBe(undefined)
                  expect(updateFound.add).toBe('boop')
                  expect(updateFound.keep).toBe('foo')

              } )
              it( 'should save and retrieve arrays correctly', async() => {
                  let array = await users.save( [ 1, 2, { foo: 'bar' } ] )
                  let foundArray = await users.findOneById( array.id )
                  expect(foundArray.id).toBe(array.id)
                  expect(foundArray[0]).toBe(array[0])
                  expect(foundArray[1]).toBe(array[1])
                  expect(foundArray[2].foo).toBe(array[2].foo)
              } )

          }

)