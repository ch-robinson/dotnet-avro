import React from 'react'

import Render from '../code/render'
import DotnetReference from '../references/dotnet'

export default ({ xml }) =>
  <Render xml={xml} map={{
    'see': ({ cref }) => <DotnetReference id={cref} />
  }} />
