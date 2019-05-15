'use strict'

const { GraphQLEnumType } = require('gatsby/graphql')

const { process } = require('../../utilities/benchmarks')

module.exports.setFieldsOnGraphQLNodeType = ({ type }) => {
  switch (type.name) {
    case 'BenchmarkResult':
      return {
        component: {
          type: new GraphQLEnumType({
            name: 'BenchmarkComponent',
            values: {
              'DESERIALIZATION': {},
              'SERIALIZATION': {}
            }
          })
        }
      }

    default:
      return {}
  }
}

module.exports.sourceNodes = async function ({ actions, createContentDigest, createNodeId }, { path }) {
  const { createNode } = actions

  const result = await process(path)
  const suites = new Map()

  for (const runtime of result.runtimes) {
    const runtimeNode = {
      id: createNodeId(runtime.name),
      name: runtime.name,
      libraries___NODE: [],
      internal: {
        content: JSON.stringify(runtime),
        contentDigest: createContentDigest(runtime),
        type: 'BenchmarkRuntime'
      }
    }

    createNode(runtimeNode)

    for (const library of runtime.libraries) {
      const libraryNode = {
        id: createNodeId(`${runtime.name}:${library.name}`),
        name: library.name,
        results___NODE: [],
        runtime___NODE: runtimeNode.id,
        internal: {
          content: JSON.stringify(library),
          contentDigest: createContentDigest(library),
          type: 'BenchmarkLibrary'
        }
      }

      createNode(libraryNode)

      for (const result of library.results) {
        let suiteNode = suites.get(result.suite)

        if (!suiteNode) {
          suiteNode = {
            id: createNodeId(result.suite),
            name: result.suite,
            results___NODE: [],
            internal: {
              content: JSON.stringify(result.suite),
              contentDigest: createContentDigest(result.suite),
              type: 'BenchmarkSuite'
            }
          }

          suites.set(result.suite, suiteNode)
          createNode(suiteNode)
        }

        const resultNode = {
          id: createNodeId(`${runtime.name}:${library.name}:${result.suite}:${result.component}`),
          component: result.component.toUpperCase(),
          iterations: result.iterations,
          times: result.times,
          library___NODE: libraryNode.id,
          suite___NODE: suiteNode.id,
          internal: {
            content: JSON.stringify(result.suite),
            contentDigest: createContentDigest(result.suite),
            type: 'BenchmarkResult'
          }
        }

        libraryNode.results___NODE.push(resultNode.id)
        suiteNode.results___NODE.push(resultNode.id)
        createNode(resultNode)
      }
    }
  }
}
