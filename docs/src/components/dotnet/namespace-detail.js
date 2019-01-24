import React from 'react'

import TypeTable from './type-table'
import XmlDoc from './xml-doc'

export default ({ namespace }) => {
  const { summary } = namespace
  const classes = namespace.types.filter(t => t.kind === 'class')
  const interfaces = namespace.types.filter(t => t.kind === 'interface')

  return (
    <>
      {summary &&
        <XmlDoc xml={summary} />
      }

      {classes.length > 0 &&
        <>
          <h3>Classes</h3>
          <TypeTable types={classes} />
        </>
      }

      {interfaces.length > 0 &&
        <>
          <h3>Interfaces</h3>
          <TypeTable types={interfaces} />
        </>
      }
    </>
  )
}
