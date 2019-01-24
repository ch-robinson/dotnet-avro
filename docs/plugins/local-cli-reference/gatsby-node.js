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

module.exports.sourceNodes = async function ({ actions, createContentDigest, createNodeId }, { path }) {
  const { createNode } = actions

  for (const verb of await load(path)) {
    const verbNode = {
      id: createNodeId(verb.name),
      examples___NODE: [],
      name: verb.name,
      options___NODE: [],
      summary: verb.summary,
      internal: {
        content: JSON.stringify(verb),
        contentDigest: createContentDigest(verb),
        type: 'CliVerb'
      }
    }

    createNode(verbNode)

    for (const example of verb.examples) {
      const exampleNode = {
        id: createNodeId(example.title),
        body: example.body,
        title: example.title,
        verb___NODE: verbNode.id,
        internal: {
          content: JSON.stringify(example),
          contentDigest: createContentDigest(example),
          type: 'CliExample'
        }
      }

      verbNode.examples___NODE.push(exampleNode.id)
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
        verb___NODE: verbNode.id,
        internal: {
          content: JSON.stringify(option),
          contentDigest: createContentDigest(option),
          type: 'CliOption'
        }
      }

      verbNode.options___NODE.push(optionNode.id)
      createNode(optionNode)
    }
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
