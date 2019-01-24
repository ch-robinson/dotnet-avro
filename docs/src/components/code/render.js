import { Fragment, createElement } from 'react'

import { parse } from '../../../utilities/xml'

export default ({ map = {}, xml }) => {
  const hydrate = (nodes = []) => nodes
    .map((node, index) => {
      switch (node.type) {
        case 'element':
          const content = hydrate(node.children)
          const match = map[node.name]
          const attributes = match ? node.attributes : {}

          return createElement(match || Fragment, { ...attributes, key: index }, content)

        case 'text':
          return node.text

        default:
          return false
      }
    })
    .filter(c => !!c)

  return hydrate(parse(`<root>${xml}</root>`).children)
}
