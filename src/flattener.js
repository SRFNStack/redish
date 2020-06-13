/**
 * Find all paths on the given object up to the specified depth
 * @param next The object to get the paths of
 * @param currPath The currentPath that we're traversing
 * @param paths The array of paths to fill up
 */
const getPathValuePairs = ( next, currPath, paths ) => {
    if( Array.isArray( next ) ) {
        if( next.length > 0 ) {
            for( let i = 0; i < next.length; i++ ) {
                getPathValuePairs( next[ i ], `${currPath}[${i}]`, paths )
            }
        }
    } else if( next && typeof next === 'object' ) {
        let keys = Object.keys( next )
        if( keys.length > 0 ) {
            keys.forEach( k => getPathValuePairs( next[ k ], currPath ? currPath + '.' + k : k, paths ) )
        }
    } else {
        paths[ currPath ] = next
    }
}

const getLastArray = ( indices, first ) => indices.length > 1 ? indices.reduce( ( arr, ind ) => {
    if( !arr[ ind ] ) arr[ ind ] = []
    return arr[ ind ]
}, first ) : first

const arrayProp = ( parent, prop ) => {
    let indMarker = prop.lastIndexOf( '[' )
    let indices = prop.slice( indMarker + 1, prop.length - 1 ).split( '][' )
    let propName = prop.slice( 0, indMarker )
    if( !Array.isArray( parent[ propName ] ) ) parent[ propName ] = []
    return {
        lastArray:  getLastArray(indices, parent[propName]),
        lastInd: indices[ indices.length - 1 ]
    }
}

const putPath = ( path, obj, val ) => {
    const parts = path.split( '.' )

    if( parts.length === 1 ) {
        obj[ parts[ 0 ] ] = val
    } else {
        const parentPaths = parts.slice( 0, parts.length - 1 )

        const getOrCreate = ( parent, prop ) => {
            if( prop.endsWith( ']' ) ) {
                const { lastArray, lastInd } = arrayProp( parent, prop )
                if( !lastArray[ lastInd ] ) lastArray[ lastInd ] = {}
                return lastArray[ lastInd ]
            } else if( !parent[ prop ] ) {
                parent[ prop ] = {}
                return parent[ prop ]
            } else {
                return parent[ prop ]
            }
        }

        const parent = parentPaths.reduce( getOrCreate, obj )
        if( parent ) {
            let prop = parts.slice( -1 )[ 0 ]
            if( prop.endsWith( ']' ) ) {
                const { lastArray, lastInd } = arrayProp( parent, prop )
                lastArray[ lastInd ] = val
            } else {
                parent[ prop ] = val
            }
        }
    }
}

module.exports = {
    /**
     * Flatten an object to a single level where the keys are json paths
     * @param obj The object to flatten
     * @returns Object A new flatter, and possibly shinier, object
     */
    flatten: ( obj ) => {
        const paths = {}
        getPathValuePairs( obj, null, paths )
        return paths
    },

    /**
     * unflatten an object to deeply nested objects
     * @param pathValuePairs
     */
    inflate( pathValuePairs ) {
        const newGuy = {}
        Object.keys( pathValuePairs ).forEach( k => putPath( k, newGuy, pathValuePairs[ k ] ) )
        return newGuy
    },

    /**
     * Get the value at the given path from the specified object
     * @param path The path to get
     * @param obj The object to get the value from
     * @returns {{}} The value at the given path
     */
    getPath: ( path, obj ) => path.split( '.' ).reduce( ( obj, prop ) => {
        if( prop.endsWith( ']' ) ) {
            let indMarker = prop.lastIndexOf( '[' )
            let ind = prop.slice( indMarker + 1, prop.length - 1 )
            return obj[ prop.slice( 0, indMarker ) ][ parseInt( ind ) ]
        } else {
            return prop in obj ? obj[ prop ] : undefined
        }
    }, obj )
}