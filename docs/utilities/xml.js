const parse = require('@rgrove/parse-xml')

/**
 * Gets the XML representation of a node.
 *
 * @param {object} node
 * A node to render markup for. If the node has children, they will be visited
 * recursively.
 *
 * @param {boolean} [include]
 * Whether to include the root node.
 *
 * @returns {string}
 * The XML representation of the node.
 */
function getMarkup (node, include = false) {
  switch (node.type) {
    case 'document':
      return node.children.map(getMarkup).join('')

    case 'element':
      const content = node.children
        .map(c => getMarkup(c, true))
        .join('')

      if (!include) {
        return content.trim()
      }

      const name = node.name
      const attributes = Object.keys(node.attributes)
        .map(k => ` ${k}="${node.attributes[k].replace(/"/, '&#34;')}"`)
        .join('')

      return `<${name}${attributes}` + (content.length > 0 ? `>${content}</${name}>` : ` />`)

    case 'text':
      return node.text

    default:
      return ''
  }
}

/**
 * Gets the inner text of an XML node.
 *
 * @param {object} node
 * A node to extract text from. If the node has children, they will be visited
 * recursively.
 *
 * @returns {string}
 * The inner text of the node with leading and trailing whitespace removed and
 * all other whitespace compacted.
 */
function getText (node) {
  switch (node.type) {
    case 'document':
    case 'element':
      return node.children.map(getText).join('')

    case 'text':
      return node.text.replace(/\s+/, ' ').trim()

    default:
      return ''
  }
}

module.exports = {
  getMarkup,
  getText,
  parse
}
