const redish = require( '../src/index.js' )

const redis = require("redis");
const client = redis.createClient(7369);
// client.auth("90d959b7-03b1-43f7-8f55-8ea716a29b2f", console.log)

redish.setClient(client)
const users = redish.collection( 'users' )

run = async ()=> {

    let orig = {
        name: 'bob',
        datas: {
            some: {
                foos: true
            },
            rayray: [ { nested: { rayray: [ true, false ]} } ],
            stuff: -9

        }
    }
    let saved = await users.save( orig )

    let found = await users.findOneById( saved.id )

    console.log( orig.datas.stuff === found.datas.stuff, saved.datas.stuff === found.datas.stuff, orig.datas.rayray[ 0 ].nested.rayray[ 0 ] === true, saved.datas.rayray[ 0 ].nested.rayray[ 0 ] === true  )

    console.log( orig.datas.stuff == found.datas.stuff,  saved.datas.stuff == found.datas.stuff, orig.datas.rayray[ 0 ].nested.rayray[ 0 ] == true, saved.datas.rayray[ 0 ].nested.rayray[ 0 ] == true )

    let update = {...found}
    delete update.datas.some.foos
    update.name = 'jerry'
    update.yup = 'yep'

    let updated = await users.save(update)

    let updateFound = await users.findOneById(updated.id)

    console.log( updateFound.name === 'jerry', Object.keys(updateFound.datas.some).length === Object.keys(update.datas.some).length, updateFound.yup ==='yep')
    return "success"
}

run().then(console.log).catch(console.error)