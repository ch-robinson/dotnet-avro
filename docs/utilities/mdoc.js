const fs = require('fs')
const path = require('path')

const {
  getMarkup,
  getText,
  parse
} = require('./xml')

const EMPTY = 'To be added.'

module.exports.process = async root => {
  const load = relative => new Promise((resolve, reject) => {
    fs.readFile(path.resolve(root, relative), (error, buffer) => {
      if (error) {
        reject(error)
      }
      else {
        try {
          const document = parse(buffer.toString())

          if (document.children.length !== 1) {
            throw new Error('Cannot parse mdoc file; document has no root element.')
          }

          resolve(document.children[0])
        }
        catch (error) {
          reject(error)
        }
      }
    })
  })

  return await processIndex(await load('index.xml'), load)
}

/**
 * Gets data from a .NET XML documentation element.
 *
 * @see https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/xmldoc/xml-documentation-comments
 *
 * @param {object} element
 * An element containing XML documentation comment nodes.
 *
 * @param {object} result
 * The object to append results to.
 *
 * @returns {object}
 * The result object.
 */
function appendDocs (element, result) {
  for (const node of element.children.filter(n => n.type === 'element')) {
    switch (node.name.toLowerCase()) {
      case 'exception':
        result.exceptions = result.exceptions || []

        const exceptionType = node.attributes['cref']
        const exceptionSummary = getMarkup(node)

        let exception = result.exceptions.find(e => e.type === exceptionType)

        if (!exception) {
          result.exceptions.push(exception = {
            type: exceptionType
          })
        }

        exception.summary = exceptionSummary
        break

      case 'param':
        result.parameters = result.parameters || []

        const parameterName = node.attributes['name']
        const parameterSummary = getMarkup(node)

        let parameter = result.parameters.find(p => p.name === parameterName)

        if (!parameter) {
          result.parameters.push(parameter = {
            parameterName
          })
        }

        parameter.summary = parameterSummary
        break

      case 'remarks':
        const remarks = getMarkup(node)

        if (remarks !== EMPTY) {
          result.remarks = remarks
        }

        break

      case 'returns':
        const returns = getMarkup(node)

        result.returns = result.returns || {}
        result.returns.summary = returns

        break

      case 'summary':
        const summary = getMarkup(node)

        if (summary !== EMPTY) {
          result.summary = summary
        }

        break

      case 'typeparam':
        result.typeParameters = result.typeParameters || []

        const typeParameterName = node.attributes['name']
        const typeParameterSummary = getMarkup(node)

        let typeParameter = result.typeParameters.find(p => p.name === typeParameterName)

        if (!typeParameter) {
          result.typeParameters.push(typeParameter = {
            name: typeParameterName
          })
        }

        typeParameter.summary = typeParameterSummary
        break
    }
  }

  return result
}

/**
 * Processes an mdoc assembly element.
 *
 * @param {object} assembly
 * An mdoc index.xml <Assembly /> element.
 *
 * @returns {Promise<object>}
 */
function processAssembly (assembly) {
  if (assembly.name !== 'Assembly') {
    throw new Error(`Invalid assembly node (expected Assembly, got ${assembly.name}).`)
  }

  return Promise.resolve({
    name: assembly.attributes['Name'],
    version: assembly.attributes['Version']
  })
}

/**
 * Processes an mdoc index document.
 *
 * @param {object} overview
 * An mdoc index.xml <Overview /> element.
 *
 * @param {(relative: string) => Promise<object>} load
 * A function to use to load dependent files.
 *
 * @returns {Promise<object>}
 */
async function processIndex (overview, load) {
  const result = {}

  for (const node of overview.children) {
    switch (node.name) {
      case 'Assemblies':
        const assemblies = node.children
          .filter(n => n.name === 'Assembly')

        result.assemblies = await Promise.all(assemblies.map(processAssembly))
        break

      case 'Types':
        const namespaces = node.children
          .filter(n => n.name === 'Namespace')

        result.namespaces = await Promise.all(namespaces.map(async namespace => {
          const name = namespace.attributes['Name']
          const element = await load(`ns-${name}.xml`)

          return await processNamespace(element)
        }))

        result.types = (await Promise.all(namespaces.map(namespace => {
          const name = namespace.attributes['Name']
          const types = namespace.children
            .filter(n => n.name === 'Type')

          return Promise.all(types.map(async type => {
            const kind = type.attributes['Kind'].toLowerCase()

            const element = await load(`${name}/${type.attributes['Name']}.xml`)
            const full = await processType(element)

            return { ...full, kind, namespace: name }
          }))
        }))).reduce((a, v) => a.concat(v), [])

        break
    }
  }

  return result
}

/**
 * Processes an mdoc member element.
 *
 * @param {object} type
 * An mdoc [namespace]/[type].xml <Member /> element.
 *
 * @returns {Promise<object>}
 */
async function processMember (member) {
  if (member.name !== 'Member') {
    throw new Error(`Invalid member node (expected Member, got ${member.name}).`)
  }

  const result = {
    name: member.attributes['MemberName']
  }

  for (const node of member.children) {
    switch (node.name) {
      case 'Docs':
        appendDocs(node, result)
        break

      case 'Implements':
        const implemented = node.children.find(n => n.name === 'InterfaceMember')

        if (implemented) {
          result.base = getText(implemented)
        }

        break

      case 'MemberSignature':
        result.memberSignatures = result.memberSignatures || []

        result.memberSignatures.push({
          language: node.attributes['Language'],
          value: node.attributes['Value']
        })

        break

      case 'MemberType':
        result.kind = getText(node).toLowerCase()
        break

      case 'Parameters':
        result.parameters = result.parameters || []

        for (const child of node.children.filter(n => n.name === 'Parameter')) {
          const name = child.attributes['Name']
          const type = child.attributes['Type']

          let parameter = result.parameters.find(p => p.name === name)

          if (!parameter) {
            result.parameters.push(parameter = {
              name
            })
          }

          parameter.type = type
        }

        break

      case 'ReturnValue':
        const returns = node.children.find(n => n.name === 'ReturnType')

        if (returns) {
          result.returns = result.returns || {}
          result.returns.type = getText(returns)
        }

        break

      case 'TypeParameters':
        result.typeParameters = result.typeParameters || []

        for (const child of node.children.filter(n => n.name === 'TypeParameter')) {
          const name = child.attributes['Name']
          const parameter = result.typeParameters.find(p => p.name === name)

          if (!parameter) {
            result.typeParameters.push({
              name
            })
          }
        }

        break
    }
  }

  return result
}

/**
 * Processes an mdoc namespace element.
 *
 * @param {object} namespace
 * An mdoc index.xml <Namespace /> element.
 *
 * @returns {Promise<object>}
 */
async function processNamespace (namespace) {
  if (namespace.name !== 'Namespace') {
    throw new Error(`Invalid namespace node (expected Namespace, got ${namespace.name}).`)
  }

  const result = {
    name: namespace.attributes['Name']
  }

  for (const node of namespace.children) {
    switch (node.name) {
      case 'Docs':
        appendDocs(node, result)
        break
    }
  }

  return result
}

/**
 * Processes an mdoc type element.
 *
 * @param {object} type
 * An mdoc [namespace]/[type].xml <Type /> element.
 *
 * @returns {Promise<object>}
 */
async function processType (type) {
  if (type.name !== 'Type') {
    throw new Error(`Invalid type node (expected Type, got ${type.name}).`)
  }

  const result = {
    name: type.attributes['Name']
  }

  for (const node of type.children) {
    switch (node.name) {
      case 'AssemblyInfo':
        const assembly = node.children.find(n => n.name === 'AssemblyName')

        if (assembly) {
          result.assembly = getText(assembly)
        }

        break

      case 'Base':
        const base = node.children.find(n => n.name === 'BaseTypeName')

        if (base) {
          result.base = getText(base)
        }

        break

      case 'Docs':
        appendDocs(node, result)
        break

      case 'Interfaces':
        result.interfaces = result.interfaces || []

        for (const child of node.children.filter(n => n.name === 'Interface')) {
          const implemented = child.children.find(n => n.name === 'InterfaceName')

          if (implemented) {
            result.interfaces.push(getText(implemented))
          }
        }

        break

      case 'Members':
        result.members = await Promise.all(node.children.filter(n => n.name === 'Member').map(processMember))
        break

      case 'TypeParameters':
        result.typeParameters = result.typeParameters || []

        for (const child of node.children.filter(n => n.name === 'TypeParameter')) {
          const name = child.attributes['Name']
          const parameter = result.typeParameters.find(p => p.name === name)

          if (!parameter) {
            result.typeParameters.push({
              name
            })
          }
        }

        break

      case 'TypeSignature':
        result.typeSignatures = result.typeSignatures || []

        result.typeSignatures.push({
          language: node.attributes['Language'],
          value: node.attributes['Value']
        })

        break
    }
  }

  return result
}
