/**
 * Creates a cref for an assembly.
 *
 * @param {string} name
 * An assembly name.
 *
 * @returns {Cref}
 */
function createAssemblyId (name) {
  return `A:${name}`
}

/**
 * Creates a bound cref for a type.
 *
 * @param {string} name
 * A type name (with or without generic type parameters).
 *
 * @param {string[]} [typeParameters]
 * A list of names that should be treated as type parameters.
 *
 * @param {string[]} [methodTypeParameters]
 * A list of names that should be treated as method type parameters.
 *
 * @returns {Cref}
 */
function createBoundTypeId (name, typeParameters = [], methodTypeParameters = []) {
  for (let index = 0; index < typeParameters.length; index++) {
    if (typeParameters[index] === name) {
      return `T:\`${index}`
    }
  }

  for (let index = 0; index < methodTypeParameters.length; index++) {
    if (methodTypeParameters[index] === name) {
      return `T:\`\`${index}`
    }
  }

  const bound = getComponents(name)
    .map(component => {
      const lower = component.indexOf('<')
      const upper = component.lastIndexOf('>')

      if (lower < 0 || upper < 0) {
        return component
      }

      const children = getTypeParameters(component)
        .map(child => createBoundTypeId(child, typeParameters, methodTypeParameters).substring(2))

      return `${component.substring(0, lower)}{${children.join(',')}}${component.substring(upper + 1)}`
    })
    .join('.')

  return `T:${bound}`
}

/**
 * Creates a URL-friendly identifier from a cref.
 *
 * @param {Cref} id
 * A bound or unbound cref.
 *
 * @returns {string}
 * An identifier that matches the style used by the Microsoft documentation and
 * DocFX.
 */
function createDocfxUrl (id) {
  const indices = ['*', '[', '(']
    .map(c => id.indexOf(c))
    .filter(i => i > -1)

  if (indices.length > 0) {
    id = id.substring(0, Math.min(...indices))
  }

  id = unbindId(id).substring(2)

  return id
    .replace(/`+/, '-')
    .replace(/#+/, '-')
    .replace(/\+/, '.')
}

/**
 * Creates a cref for an event.
 *
 * @param {string} name
 * An event name.
 *
 * @returns {Cref}
 */
function createEventId (name) {
  return `E:${name}`
}

/**
 * Creates a cref for a field.
 *
 * @param {string} name
 * A field name.
 *
 * @returns {Cref}
 */
function createFieldId (name) {
  const components = getComponents(name)

  let field = components.pop()
  let type = components.pop()

  const typeParameters = getTypeParameters(type)

  if (type.indexOf('<') > 0) {
    type = `${type.substring(0, type.indexOf('<'))}\`${typeParameters.length}${type.substring(type.lastIndexOf('>') + 1)}`
  }

  return `F:${[...components, type, field].join('.')}`
}

/**
 * Creates a display name for a field, property, or method.
 *
 * @param {Cref} id
 * A member cref.
 *
 * @returns {string}
 */
function createMemberName (id) {
  id = id.substring(2)

  const components = getComponents(id)

  if (components.length < 2) {
    return ''
  }

  id = components.pop()

  if (id.startsWith('#ctor')) {
    return createTypeName(components.join('.'))
  }

  const indices = ['`', '{', '(']
    .map(c => id.indexOf(c))
    .filter(i => i > -1)

  if (indices.length > 0) {
    id = id.substring(0, Math.min(...indices))
  }

  return id
}

/**
 * Creates a cref for a namespace.
 *
 * @param {string} name
 * A namespace.
 *
 * @returns {string}
 */
function createNamespaceId (name) {
  return `N:${name}`
}

/**
 * Creates a cref for a property.
 *
 * @param {string} name
 * A property name.
 *
 * @returns {Cref}
 */
function createPropertyId (name) {
  const components = getComponents(name)

  let property = components.pop()
  let type = components.pop()

  const typeParameters = getTypeParameters(type)

  if (type.indexOf('<') > 0) {
    type = `${type.substring(0, type.indexOf('<'))}\`${typeParameters.length}${type.substring(type.lastIndexOf('>') + 1)}`
  }

  return `P:${[...components, type, property].join('.')}`
}

/**
 * Creates a display name for a type.
 *
 * @param {Cref} id
 * A type cref.
 *
 * @returns {string}
 */
function createTypeName (id) {
  id = id.substring(2)

  const primitiveIndices = ['*', '[']
    .map(c => id.indexOf(c))
    .filter(i => i > -1)

  if (primitiveIndices.length > 0) {
    id = id.substring(0, Math.min(...primitiveIndices))
  }

  switch (id) {
    case 'System.Boolean': return 'bool'
    case 'System.Byte': return 'byte'
    case 'System.Char': return 'char'
    case 'System.Decimal': return 'decimal'
    case 'System.Double': return 'double'
    case 'System.Int16': return 'short'
    case 'System.Int32': return 'int'
    case 'System.Int64': return 'long'
    case 'System.Object': return 'object'
    case 'System.SByte': return 'sbyte'
    case 'System.Single': return 'float'
    case 'System.String': return 'string'
    case 'System.UInt16': return 'ushort'
    case 'System.UInt32': return 'uint'
    case 'System.UInt64': return 'ulong'
    case 'System.Void': return 'void'

    default:
      const components = getComponents(id)

      if (components.length < 1) {
        return ''
      }

      id = components.pop()

      const genericIndices = ['`', '{']
        .map(c => id.indexOf(c))
        .filter(i => i > -1)

      if (genericIndices.length > 0) {
        id = id.substring(0, Math.min(...genericIndices))
      }

      return id
  }
}

/**
 * Creates an unbound cref for a method in generic form.
 *
 * @param {string} name
 * A method name.
 *
 * @returns {Cref}
 */
function createUnboundMethodId (name) {
  const components = getComponents(name)

  let method = components.pop()
  let type = components.pop()

  const methodParameters = getMethodParameters(method)
  const methodTypeParameters = getTypeParameters(method)

  const typeParameters = getTypeParameters(type)

  if (method.indexOf('(') > 0) {
    method = `${method.substring(0, method.indexOf('('))}(${methodParameters.map(t => createBoundTypeId(t, typeParameters, methodTypeParameters)).join(',')})`
  }

  if (method.indexOf('<') > 0) {
    method = `${method.substring(0, method.indexOf('<'))}\`\`${methodTypeParameters.length}${method.substring(method.lastIndexOf('>') + 1)}`
  }

  if (type.indexOf('<') > 0) {
    type = `${type.substring(0, type.indexOf('<'))}\`${typeParameters.length}${type.substring(type.lastIndexOf('>') + 1)}`
  }

  return `M:${[...components, type, method].join('.')}`
}

/**
 * Creates an unbound cref for a type in generic form.
 *
 * @see https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/xmldoc/cref-attribute
 *
 * @param {string} name
 * A type name (with or without generic type parameters).
 *
 * @returns {Cref}
 */
function createUnboundTypeId (name) {
  const components = getComponents(name)

  let type = components.pop(), typeParameters = getTypeParameters(type)

  if (type.indexOf('<') > 0) {
    type = `${type.substring(0, type.indexOf('<'))}\`${typeParameters.length}${type.substring(type.lastIndexOf('>') + 1)}`
  }

  return `T:${[...components, type].join('.')}`
}

/**
 * Gets the components of an identifier.
 *
 * @param {string} id
 * A cref or a name.
 *
 * @returns {string[]}
 */
function getComponents (id) {
  let level = 0
  let start = 0
  let components = []

  for (let index = 0; index < id.length; index++) {
    switch (id[index]) {
      case '<':
      case '{':
      case '(':
        level += 1
        break

      case '>':
      case '}':
      case ')':
        level -= 1
        break

      case '.':
      case '+':
        if (level === 0) {
          components.push(id.substring(start, index))
          start = index + 1
        }

        break

      default:
        //
    }
  }

  components.push(id.substring(start, id.length))
  return components
}

/**
 * Gets the method parameters from an identifier.
 *
 * @param {string} id
 * A cref or a name.
 *
 * @returns {string[]}
 * An array of method parameters.
 */
function getMethodParameters (id) {
  let level = 0
  let parameters = new Set()

  const select = (start, index) => {
    const trimmed = id.substring(start, index).trim()

    if (trimmed) {
      parameters.add(trimmed)
    }
  }

  for (let index = 0, start = 0; index < id.length; index++) {
    switch (id[index]) {
      case '(':
        if (level === 0) {
          start = index + 1
        }

        level += 1
        break

      case ')':
        if (level === 1) {
          select(start, index)
        }

        level -= 1
        break

      case ',':
        if (level === 1) {
          select(start, index)
          start = index + 1
        }

        break

      case '<':
      case '{':
        level += 1
        break

      case '>':
      case '}':
        level -= 1
        break

      default:
        //
    }
  }

  if (level) {
    return null
  }

  return Array.from(parameters)
}

/**
 * Gets the type parameters from an identifier.
 *
 * @param {string} id
 * A cref or a name.
 *
 * @returns {string[]}
 * An array of type parameters or null if the type is not generic.
 */
function getTypeParameters (id) {
  let level = 0
  let parameters = new Set()

  const select = (start, index) => {
    const trimmed = id.substring(start, index).trim()

    if (trimmed) {
      parameters.add(trimmed)
    }
  }

  for (let index = 0, start = 0; index < id.length; index++) {
    switch (id[index]) {
      case '<':
      case '{':
        if (level === 0) {
          start = index + 1
        }

        level += 1
        break

      case '>':
      case '}':
        if (level === 1) {
          select(start, index)
        }

        level -= 1
        break

      case ',':
        if (level === 1) {
          select(start, index)
          start = index + 1
        }

        break

      case '(':
        level += 1
        break

      case ')':
        level -= 1
        break

      default:
        //
    }
  }

  if (level || !parameters.size) {
    return null
  }

  return Array.from(parameters)
}

/**
 * Gets the suffix ([], *, etc.) for a type.
 *
 * @param {string} id
 * A cref.
 *
 * @returns {string}
 * The type suffix or an empty string.
 */
function getTypeSuffix (id) {
  const match = id.match(/([:,^*[\]]+[\d:,^*[\]]*)$/)

  if (match) {
    return match[1]
  }

  return ''
}

/**
 * Concatenates a namespace and a name.
 *
 * @param {string} name
 * A name in generic form.
 *
 * @param {string} namespace
 * A namespace.
 *
 * @returns {string}
 */
function qualifyName (name, namespace) {
  return namespace ? `${namespace}.${name}` : name
}

/**
 * Unbinds a cref.
 *
 * @param {string} id
 * A bound cref.
 *
 * @returns {string}
 */
function unbindId (id) {
  return getComponents(id)
    .map(component => {
      const typeParameters = getTypeParameters(component)

      if (!typeParameters || typeParameters.length < 1) {
        return component
      }

      return `${component.substring(0, component.indexOf('{'))}\`${getTypeParameters(component).length}${component.substring(component.lastIndexOf('}') + 1)}`
    })
    .join('.')
}

module.exports = {
  createAssemblyId,
  createBoundTypeId,
  createDocfxUrl,
  createEventId,
  createFieldId,
  createMemberName,
  createNamespaceId,
  createPropertyId,
  createTypeName,
  createUnboundMethodId,
  createUnboundTypeId,
  getMethodParameters,
  getTypeParameters,
  getTypeSuffix,
  qualifyName,
  unbindId
}

/**
 * Code references, also referred to as "string IDs" by the C# spec (ECMA-334),
 * consist of a member type prefix, the fully qualified type name, and, when
 * applicable, member names, type parameters, method parameters, etc.
 *
 * Member type prefixes include:
 * - E: event
 * - F: field
 * - M: method/constructor
 * - N: namespace
 * - P: property
 * - T: type
 *
 * @see http://docs.go-mono.com/?link=man%3amdoc(5)
 * @see https://www.ecma-international.org/publications/files/ECMA-ST/ECMA-334.pdf (Annex D.4.2, pp. 484-485)
 *
 * @typedef {string} Cref
 */
