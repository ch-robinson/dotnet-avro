'use strict'

module.exports.createPages = async function ({ graphql, actions }, { verbComponent }) {
  const { createPage } = actions

  const query = await graphql(`
    {
      allCliVerb {
        edges {
          node {
            id
            name
          }
        }
      }
    }
  `)

  for (const verb of query.data.allCliVerb.edges.map(e => e.node)) {
    createPage({
      component: verbComponent,
      context: {
        id: verb.id
      },
      path: `/cli/${verb.name}/`,
    })
  }
}

module.exports.createSchemaCustomization = function ({ actions }) {
  const { createTypes } = actions

  createTypes(`
    type CliExample implements Node {
      id: ID!
      language: String!
      title: String!
      body: String!
      verb: CliVerb! @link
    }

    type CliOption implements Node {
      id: ID!
      abbreviation: String
      name: String!
      required: Boolean!
      set: String
      summary: String
      verb: CliVerb! @link
    }

    type CliVerb implements Node {
      id: ID!
      name: String!
      summary: String
      examples: [CliExample!] @link
      options: [CliOption!] @link
    }
  `)
}

module.exports.sourceNodes = async function ({ actions, createContentDigest, createNodeId }, { path }) {
  const { createNode } = actions

  for (const verb of await load(path)) {
    const verbNode = {
      id: createNodeId(verb.name),
      examples: [],
      name: verb.name,
      options: [],
      summary: verb.summary,
      internal: {
        content: JSON.stringify(verb),
        contentDigest: createContentDigest(verb),
        type: 'CliVerb'
      }
    }

    for (const example of verb.examples) {
      const exampleNode = {
        id: createNodeId(example.title),
        body: example.body,
        language: example.language || 'bash',
        title: example.title,
        verb: verbNode.id,
        internal: {
          content: JSON.stringify(example),
          contentDigest: createContentDigest(example),
          type: 'CliExample'
        }
      }

      verbNode.examples.push(exampleNode.id)
      createNode(exampleNode)
    }

    for (const option of verb.options) {
      const optionNode = {
        id: createNodeId(option.name),
        abbreviation: option.abbreviation,
        name: option.name,
        required: option.required,
        set: option.set,
        summary: option.summary,
        verb: verbNode.id,
        internal: {
          content: JSON.stringify(option),
          contentDigest: createContentDigest(option),
          type: 'CliOption'
        }
      }

      verbNode.options.push(optionNode.id)
      createNode(optionNode)
    }

    createNode(verbNode)
  }
}

/**
 * Loads CLI data.
 *
 * @param {string} path
 * The absolute path to a CLI data file (JS/JSON).
 *
 * @returns {Promise<object>}
 */
function load (path) {
  return Promise.resolve(require(path))
}
