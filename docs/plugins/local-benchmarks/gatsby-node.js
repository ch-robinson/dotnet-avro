'use strict'

const { GraphQLEnumType } = require('gatsby/graphql')

const { process } = require('../../utilities/benchmarks')

module.exports.createSchemaCustomization = function ({ actions }) {
  const { createTypes } = actions

  createTypes(`
    enum BenchmarkComponent {
      DESERIALIZATION
      SERIALIZATION
    }

    type BenchmarkLibrary implements Node {
      id: ID!
      name: String!
      results: [BenchmarkResult!]! @link
    }

    type BenchmarkResult implements Node {
      id: ID!
      component: BenchmarkComponent!
      iterations: Int!
      library: BenchmarkLibrary! @link
      suite: BenchmarkSuite! @link
      times: [Float!]!
    }

    type BenchmarkRuntime implements Node {
      id: ID!
      name: String!
      libraries: [BenchmarkLibrary!]! @link
    }

    type BenchmarkSuite implements Node {
      id: ID!
      name: String!
      results: [BenchmarkResult!]! @link
    }
  `)
}

module.exports.sourceNodes = async function ({ actions, createContentDigest, createNodeId }, { path }) {
  const { createNode } = actions

  const result = await process(path)
  const suites = new Map()

  for (const runtime of result.runtimes) {
    const runtimeNode = {
      id: createNodeId(runtime.name),
      name: runtime.name,
      libraries: [],
      internal: {
        content: JSON.stringify(runtime),
        contentDigest: createContentDigest(runtime),
        type: 'BenchmarkRuntime'
      }
    }

    for (const library of runtime.libraries) {
      const libraryNode = {
        id: createNodeId(`${runtime.name}:${library.name}`),
        name: library.name,
        results: [],
        runtime: runtimeNode.id,
        internal: {
          content: JSON.stringify(library),
          contentDigest: createContentDigest(library),
          type: 'BenchmarkLibrary'
        }
      }

      for (const result of library.results) {
        let suiteNode = suites.get(result.suite)

        if (!suiteNode) {
          suiteNode = {
            id: createNodeId(result.suite),
            name: result.suite,
            results: [],
            internal: {
              content: JSON.stringify(result.suite),
              contentDigest: createContentDigest(result.suite),
              type: 'BenchmarkSuite'
            }
          }

          suites.set(result.suite, suiteNode)
        }

        const resultNode = {
          id: createNodeId(`${runtime.name}:${library.name}:${result.suite}:${result.component}`),
          component: result.component.toUpperCase(),
          iterations: result.iterations,
          times: result.times,
          library: libraryNode.id,
          suite: suiteNode.id,
          internal: {
            content: JSON.stringify(result.suite),
            contentDigest: createContentDigest(result.suite),
            type: 'BenchmarkResult'
          }
        }

        libraryNode.results.push(resultNode.id)
        suiteNode.results.push(resultNode.id)
        createNode(resultNode)
      }

      createNode(libraryNode)
    }

    createNode(runtimeNode)
  }

  for (const suiteNode of suites.values()) {
    createNode(suiteNode)
  }
}
