const redish = require('./src/index.js')
const ObjectID = require('isomorphic-mongo-objectid')
const redis = require('redis')
const client = redis.createClient({ url: 'redis://localhost:6669' })
const db = redish.createDb(client)
const foo = db.collection('foo')
beforeAll(async () => {
  await client.connect()
})
afterAll(() => client.quit())
describe(
  'redish',
  () => {
    it('should save and retrieve complex objects containing all primitive objects correctly', async () => {
      const orig = {
        emptyObject: {},
        emptyArray: [],
        emptyString: '',
        null: null,
        undefined,
        boolean: true,
        string: 'str',
        BigInt: BigInt('420420420420420420'),
        symbol: Symbol.for('foo'),
        number: 1,
        date: new Date(),
        nestedArrays: [0, [0, [0, { a: [[[0]]] }]]],
        nestedObjects: {
          a: 5,
          b: {
            a: 5,
            b: {
              a: 5,
              b: 6,
              c: [0, [[[1]]]]
            }
          }
        }
      }
      const saved = await foo.save(orig)
      const found = await foo.findOneById(saved.id)
      const objectId = saved.id.split('foo__')[1]
      expect(ObjectID(objectId).toString()).toBe(objectId)
      expect(saved.id).toBe(found.id)
      for (const result of [saved, found]) {
        expect(result.emptyObject).toStrictEqual(orig.emptyObject)
        expect(result.emptyArray).toStrictEqual(orig.emptyArray)
        expect(result.emptyString).toStrictEqual(orig.emptyString)
        expect(result.null).toStrictEqual(orig.null)
        expect(result.undefined).toStrictEqual(orig.undefined)
        expect(result.boolean).toStrictEqual(orig.boolean)
        expect(result.string).toStrictEqual(orig.string)
        expect(result.BigInt).toStrictEqual(orig.BigInt)
        expect(result.symbol).toStrictEqual(orig.symbol)
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
    })

    it('should delete keys that are deleted from objects', async () => {
      const update = await foo.save({ keep: 'foo', del: 'bar' })
      delete update.del
      update.add = 'boop'

      const updated = await foo.save(update)

      const updateFound = await foo.findOneById(updated.id)
      expect(updateFound.id).toBe(update.id)
      expect(updateFound.del).toBe(undefined)
      expect(updateFound.add).toBe('boop')
      expect(updateFound.keep).toBe('foo')
    })

    it('should not delete keys that are deleted from objects when using upsert', async () => {
      const update = await foo.upsert({ keep: 'foo', del: 'bar' })
      delete update.del
      update.add = 'boop'

      const updated = await foo.upsert(update)

      const updateFound = await foo.findOneById(updated.id)
      expect(updateFound.id).toBe(update.id)
      expect(updateFound.del).toBe('bar')
      expect(updateFound.add).toBe('boop')
      expect(updateFound.keep).toBe('foo')
    })

    it('should save and retrieve arrays correctly', async () => {
      const array = await foo.save([1, 2, { foo: 'bar' }])
      const foundArray = await foo.findOneById(array.id)
      expect(foundArray.id).toBe(array.id)
      expect(foundArray[0]).toBe(array[0])
      expect(foundArray[1]).toBe(array[1])
      expect(foundArray[2].foo).toBe(array[2].foo)
    })

    it('should be able to find all of the items in a collection', async () => {
      const bar = db.collection('bar' + new Date().getTime())
      const saved = await Promise.all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(k => bar.save({ k })))
      const found = await bar.findAll()
      expect(found).toStrictEqual(saved)
    })

    it('should page results correctly', async () => {
      const bar = db.collection('bar' + new Date().getTime())
      for (const i of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) {
        await bar.save({ i })
      }
      for (let i = 0; i < 10; i++) {
        const found = await bar.findAll(i, 1)
        expect(found.length).toBe(1)
        expect(found[0].i).toBe(i + 1)
      }
    })

    it('should page results in reverse', async () => {
      const bar = db.collection('bar' + new Date().getTime())
      for (const i of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) {
        await bar.save({ i })
      }
      for (let i = 0; i < 10; i++) {
        const found = await bar.findAll(i, 1, true)
        expect(found.length).toBe(1)
        expect(found[0].i).toBe(10 - i)
      }
    })

    it('should be able to delete records correctly', async () => {
      const saved = await foo.save({ yep: true })
      const beforeDelete = await foo.findOneById(saved.id)
      expect(saved).toStrictEqual(beforeDelete)
      await foo.deleteById(saved.id)
      const found = await foo.findOneById(saved.id)
      expect(found).toStrictEqual(null)
    })
  }
)
