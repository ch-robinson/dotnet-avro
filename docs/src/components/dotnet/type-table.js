import React, { Fragment } from 'react'

import XmlDoc from '../dotnet/xml-doc'
import DotnetReference from '../references/dotnet'

export default ({ types }) =>
  types.length > 0 && types.map(type =>
    <Fragment key={type.id}>
      <h5>
        <DotnetReference id={type.id} />
      </h5>
      <XmlDoc xml={type.summary} />
    </Fragment>
  )
