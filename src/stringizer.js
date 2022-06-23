const { jsonPathPathExpander, jsonPathPathReducer } = require('jpflat')
const getTypeKey = value => value.substr(-1)
const removeTypeKey = value => value.slice(0, -2)
const typeKeys = {
  emptyObject: '0',
  emptyArray: '1',
  emptyString: '2',
  null: '3',
  undefined: '4',
  boolean: '5',
  string: '6',
  BigInt: '7',
  Symbol: '8',
  number: 'a',
  Date: 'b'
}

const stringizers = {
  [typeKeys.emptyObject]: {
    to: () => '{}',
    from: () => ({})
  },
  [typeKeys.emptyArray]: {
    to: () => '[]',
    from: () => []
  },
  [typeKeys.emptyString]: {
    to: () => '\'\'',
    from: () => ''
  },
  [typeKeys.null]: {
    to: () => 'null',
    from: () => null
  },
  [typeKeys.undefined]: {
    to: () => 'undefined',
    from: () => undefined
  },
  [typeKeys.boolean]: {
    to: o => String(o),
    from: s => Boolean(s)
  },
  [typeKeys.string]: {
    to: o => String(o),
    from: s => s
  },
  [typeKeys.BigInt]: {
    to: o => String(o),
    from: s => BigInt(s)
  },
  [typeKeys.Symbol]: {
    to: o => String(o),
    from: s => Symbol.for(s.substring('Symbol('.length, s.length - 1))
  },
  [typeKeys.number]: {
    to: o => String(o),
    from: s => Number(s)
  },
  [typeKeys.Date]: {
    to: o => o.toISOString(),
    from: s => new Date(s)
  }
}

const chooseTypeKey = o => {
  if (o === undefined) return typeKeys.undefined
  if (o === null) return typeKeys.null
  if (typeof o === 'string' && o.length === 0) return typeKeys.emptyString
  if (Array.isArray(o) && o.length === 0) return typeKeys.emptyArray
  if (typeKeys[typeof o]) return typeKeys[typeof o]
  if (typeKeys[o.constructor.name]) return typeKeys[o.constructor.name]
  if (typeof o === 'object' && Object.keys(o).length === 0) return typeKeys.emptyObject
  return undefined
}

/**
 * This jpflat serializer/deserializer supports basic types in javascript and treats everything else as a plain string
 * @type {{serialize: (function(*=): string), canDeserialize: (function(*=): *), canSerialize: (function(*=): *), deserialize: (function(*=): *)}}
 */
module.exports = {
  canSerialize: (pathParts, o) => !!chooseTypeKey(o),
  canDeserialize: () => true,
  serialize: (pathParts, o) => {
    const typeKey = chooseTypeKey(o) || typeKeys.string
    // this has to be done before the value is stringized or else it will always be string type
    pathParts.push({ typeKey })
    return stringizers[typeKey].to(o)
  },
  deserialize: (path, value) => stringizers[getTypeKey(path)].from(value),
  pathExpander: (path) => jsonPathPathExpander(removeTypeKey(path)),
  pathReducer: (pathParts) => jsonPathPathReducer(pathParts.slice(0, -1)) + ':' + (pathParts.pop().typeKey),
  typeKeys,
  stringizers,
  getTypeKey,
  removeTypeKey
}
