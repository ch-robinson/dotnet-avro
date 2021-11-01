import React from 'react'

import Render from '../code/render'
import DotnetReference from '../references/dotnet'

export default function XmlDoc ({ xml }) {
  return (
    <Render xml={xml} map={{
      'see': ({ cref }) => <DotnetReference id={cref} />
    }} />
  )
}
