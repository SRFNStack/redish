const redish = require( '../src/index.js' )

const redis = require("redis");
const client = redis.createClient();
client.auth("secrets", console.log)

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

    let found = await users.findOneByKey( saved.key )

    console.log( orig.datas.stuff === found.datas.stuff, saved.datas.stuff === found.datas.stuff, orig.datas.rayray[ 0 ].nested.rayray[ 0 ] === true, saved.datas.rayray[ 0 ].nested.rayray[ 0 ] === true  )

    console.log( orig.datas.stuff == found.datas.stuff,  saved.datas.stuff == found.datas.stuff, orig.datas.rayray[ 0 ].nested.rayray[ 0 ] == true, saved.datas.rayray[ 0 ].nested.rayray[ 0 ] == true )
}

run().then(console.log).catch(console.error)