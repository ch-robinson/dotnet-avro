'use strict'

const {
  createAssemblyId,
  createDocfxUrl,
  createFieldId,
  createNamespaceId,
  createPropertyId,
  createBoundTypeId,
  createUnboundMethodId,
  createUnboundTypeId,
  qualifyName
} = require('../../utilities/dotnet')

const { process } = require('../../utilities/mdoc')

module.exports.createPages = async function ({ graphql, actions }, { memberComponent, namespaceComponent, typeComponent }) {
  const { createPage } = actions

  const query = await graphql(`
    {
      allDotnetNamespace {
        edges {
          node {
            id
            types {
              id
              members {
                id
              }
            }
          }
        }
      }
    }
  `)

  for (const namespace of query.data.allDotnetNamespace.edges.map(e => e.node)) {
    createPage({
      component: namespaceComponent,
      context: {
        id: namespace.id
      },
      path: `/api/${createDocfxUrl(namespace.id).toLowerCase()}`
    })

    for (const type of namespace.types) {
      createPage({
        component: typeComponent,
        context: {
          id: type.id
        },
        path: `/api/${createDocfxUrl(type.id).toLowerCase()}`
      })

      for (const member of type.members) {
        createPage({
          component: memberComponent,
          context: {
            id: member.id
          },
          path: `/api/${createDocfxUrl(member.id).toLowerCase()}`
        })
      }
    }
  }
}

module.exports.sourceNodes = async function ({ actions, createContentDigest }, { path }) {
  const { createNode } = actions

  const result = await process(path)
  const assemblies = new Map()
  const namespaces = new Map()

  for (const assembly of result.assemblies) {
    const assemblyNode = {
      id: createAssemblyId(assembly.name),
      ...assembly,
      types___NODE: [],
      internal: {
        content: JSON.stringify(assembly),
        contentDigest: createContentDigest(assembly),
        type: 'DotnetAssembly'
      }
    }

    assemblies.set(assembly.name, assemblyNode)
    createNode(assemblyNode)
  }

  for (const namespace of result.namespaces) {
    const namespaceNode = {
      id: createNamespaceId(namespace.name),
      ...namespace,
      types___NODE: [],
      internal: {
        content: JSON.stringify(namespace),
        contentDigest: createContentDigest(namespace),
        type: 'DotnetNamespace'
      }
    }

    namespaces.set(namespace.name, namespaceNode)
    createNode(namespaceNode)
  }

  for (const type of result.types) {
    const { assembly, members, namespace, ...properties } = type

    const typeParameters = (type.typeParameters || []).map(p => p.name)

    if (properties.base) {
      properties.base = createBoundTypeId(properties.base, typeParameters);
    }

    if (properties.interfaces) {
      properties.interfaces = properties.interfaces.map(i => createBoundTypeId(i, typeParameters))
    }

    const assemblyNode = assemblies.get(assembly)
    const namespaceNode = namespaces.get(namespace)

    const typeNode = {
      id: createUnboundTypeId(qualifyName(properties.name, namespace)),
      ...properties,
      assembly___NODE: assemblyNode.id,
      members___NODE: [],
      namespace___NODE: namespaceNode.id,
      internal: {
        content: JSON.stringify(type),
        contentDigest: createContentDigest(type),
        type: 'DotnetType'
      }
    }

    assemblyNode.types___NODE.push(typeNode.id)
    namespaceNode.types___NODE.push(typeNode.id)
    createNode(typeNode)

    if (members) {
      const overloads = members.reduce((result, member) => {
        const { kind, name, ...others } = member

        if (!result[name]) {
          result[name] = {
            kind,
            name: name === '.ctor' ? '#ctor' : name,
            overloads: []
          }
        }

        result[name].overloads.push(others)
        return result
      }, new Map())

      for (const name in overloads) {
        const memberNode = {
          id: `${qualifyName(typeNode.name, namespace)}.${overloads[name].name}`,
          kind: overloads[name].kind,
          name: overloads[name].name,
          overloads: overloads[name].overloads,
          type___NODE: typeNode.id,
          internal: {
            content: JSON.stringify(overloads[name]),
            contentDigest: createContentDigest(overloads[name]),
            type: 'DotnetMember'
          }
        }

        switch (memberNode.kind) {
          case 'constructor':
          case 'method':
            memberNode.id = createUnboundMethodId(memberNode.id)
            break

          case 'field':
            memberNode.id = createFieldId(memberNode.id)
            break

          case 'property':
            memberNode.id = createPropertyId(memberNode.id)
            break

          default:
            throw new Error(`Unexpected kind ${memberNode.kind}.`)
        }

        typeNode.members___NODE.push(memberNode.id)
        createNode(memberNode)

        for (const overload of memberNode.overloads) {
          overload.id = memberNode.id

          const methodTypeParameters = (overload.typeParameters || []).map(p => p.name)

          if (overload.parameters) {
            for (const parameter of overload.parameters) {
              parameter.type = createBoundTypeId(parameter.type, typeParameters, methodTypeParameters)
            }

            overload.id += `(${overload.parameters.map(p => p.type.substring(2)).join(',')})`
          }

          if (overload.returns) {
            overload.returns.type = createBoundTypeId(overload.returns.type, typeParameters, methodTypeParameters)
          }
        }
      }
    }
  }
}
