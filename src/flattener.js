/**
 * Find all paths on the given object up to the specified depth
 * @param next The object to get the paths of
 * @param currPath The currentPath that we're traversing
 * @param paths The array of paths to fill up
 * @param filter A filter to use to avoid traversing properties. i.e. Promises
 */
const toPathValuePairs = ( next, currPath, paths, filter ) => {
  if( Array.isArray( next ) ) {
    if( next.length > 0 ) {
      for( let i = 0; i < next.length; i++ ) {
        toPathValuePairs( next[ i ], `${currPath}[${i}]`, paths, filter )
      }
    }
  } else if( next && typeof next === 'object' && filter( next ) ) {
    let keys = Object.keys( next )
    if( keys.length > 0 ) {
      keys.forEach( k => toPathValuePairs( next[ k ], currPath ? currPath + '.' + k : k, paths,  filter ) )
    } else {
      paths.push( currPath )
    }
  } else {
    paths.push( currPath )
  }
}

export default {
  /**
   * Convert an object to a list of paths up to the given depth
   * @param obj The object to get the paths of
   * @param filter A filter to use to avoid traversing properties. i.e. Promises
   * @param depth The depth to go
   * @returns {Array} The array of paths
   */
  toPaths: ( obj, filter, depth ) => {
    const paths = []
    let pass = () => true
    toPathValuePairs( obj, null, paths, depth, filter || pass )
    return paths
  },

  /**
   * Set the value of the given object at the specified path
   * @param path The path to set the value at
   * @param obj The object to set the value on
   * @param val The value to set
   */
  putPath: ( path, obj, val ) => {
    const parts = path.split( '.' )

    if( parts.length === 1 ) {
      obj[ parts[ 0 ] ] = val
    } else {
      const parentPaths = parts.slice( 0, parts.length - 1 )
      const parent = parentPaths
          .reduce( ( obj, prop ) => {
            if( !obj[ prop ] ) {
              if( prop.endsWith( ']' ) ) {
                obj[ prop ] = []
              } else {
                obj[ prop ] = {}
              }
              return obj[ prop ]
            } else {
              if( prop.endsWith( ']' ) ) {
                let indMarker = prop.lastIndexOf( '[' )
                let ind = prop.slice( indMarker + 1, prop.length - 1 )
                let arrProp = prop.slice( 0, indMarker )
                if( !Array.isArray( obj[ arrProp ] ) ) obj[ arrProp ] = []
                return obj[ arrProp ][ parseInt( ind ) ]
              } else {
                return obj[ prop ]
              }
            }
          }, obj )
      if( parent ) {
        let prop = parts.slice( -1 )[ 0 ]
        if( prop.endsWith( ']' ) ) {
          let indMarker = prop.lastIndexOf( '[' )
          let ind = prop.slice( indMarker + 1, prop.length - 1 )
          parent[ prop.slice( 0, indMarker ) ][ ind ] = val
        } else {
          parent[ prop ] = val
        }
      } else {
        return parent
      }
    }
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
  }, obj ),

  /**
   * Check whether the given path actually exists on an object
   * @param path The path to check
   * @param obj The object to check if the path exists on
   * @returns boolean whether the path exists or not
   */
  hasPath: ( path, obj ) => {
    let parent = path.split( '.' ).slice( 0, -1 )
    if( parent && parent.length > 1 ) {
      return !!this.getPath( parent.join( '.' ), obj )
    } else {
      return !!obj && obj.hasOwnProperty( path )
    }
  }
}