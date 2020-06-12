const redish = require( '../src/index.js' )


const users = redish.collection( 'users' )

saved = users.save( {
                name: 'bob',
                datas: {
                    some: {
                        foos: true
                    },
                    rayray: [ 1, 'a', { nested: { rayray: [ true, false ], nums: 1.1 } } ],
                    stuff: -9

                }
            } )

found = users.findOneByKey(saved.key)

console.log(saved.name === found.name, saved.datas.rayray[2].nested.rayray[0] === true)