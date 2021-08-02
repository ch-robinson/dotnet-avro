import React, { Fragment } from 'react'

import XmlDoc from '../dotnet/xml-doc'
import DotnetReference from '../references/dotnet'

export default function MemberTable ({ members, typeParameters }) {
  typeParameters = (typeParameters || []).map(p => p.name)

  return members.length > 0 && members.map(member =>
    <Fragment key={member.id}>
      {member.overloads.map(overload => {
        const methodTypeParameters = member.kind === 'method' && overload.typeParameters
          ? overload.typeParameters.map(p => p.name)
          : null

        return (
          <Fragment key={overload.id}>
            <h5>
              {overload.returns &&
                <><DotnetReference id={overload.returns.type} typeParameters={typeParameters} methodTypeParameters={methodTypeParameters} /> </>
              }

              <DotnetReference id={overload.id} typeParameters={typeParameters} methodTypeParameters={methodTypeParameters} />
            </h5>
            <XmlDoc xml={overload.summary} />
          </Fragment>
        )
      })}
    </Fragment>
  )
}
