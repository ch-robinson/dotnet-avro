import React, { Fragment } from 'react'

import DotnetReference from '../references/dotnet'

import InheritanceTable from './inheritance-table'
import XmlDoc from './xml-doc'

export default ({ member }) => {
  const { exceptions, kind, parameters, remarks, returns, summary, type, typeParameters } = member

  const typeParameterNames = (type.typeParameters || []).map(p => p.name)
  const methodTypeParameterNames = (typeParameters || []).map(p => p.name)

  return (
    <>
      {summary &&
        <XmlDoc xml={summary} />
      }

      <InheritanceTable member={member} />

      {remarks &&
        <XmlDoc xml={remarks} />
      }

      {typeParameters && typeParameters.length > 0 &&
        <>
          <h3>Type parameters</h3>
          {typeParameters.map((parameter, index) =>
            <Fragment key={index}>
              <h5>{parameter.name}</h5>
              {parameter.summary && <XmlDoc xml={parameter.summary} />}
            </Fragment>
          )}
        </>
      }

      {parameters && parameters.length > 0 &&
        <>
          <h3>Parameters</h3>
          {parameters.map((parameter, index) =>
            <Fragment key={index}>
              <h5><DotnetReference id={parameter.type} typeParameters={typeParameterNames} methodTypeParameters={methodTypeParameterNames} /> {parameter.name}</h5>
              {parameter.summary && <XmlDoc xml={parameter.summary} />}
            </Fragment>
          )}
        </>
      }

      {returns &&
        <>
          <h3>{kind === 'method'
            ? 'Return type'
            : 'Type'
          }</h3>
          <h5><DotnetReference id={returns.type} typeParameters={typeParameterNames} methodTypeParameters={methodTypeParameterNames} /></h5>
          {returns.summary && <XmlDoc xml={returns.summary} />}
        </>
      }

      {exceptions && exceptions.length > 0 &&
        <>
          <h3>Exceptions</h3>
          {exceptions.map((exception, index) =>
            <Fragment key={index}>
              <h5><DotnetReference id={exception.type} typeParameters={typeParameterNames} methodTypeParameters={methodTypeParameterNames} /></h5>
              {exception.summary && <XmlDoc xml={exception.summary} />}
            </Fragment>
          )}
        </>
      }
    </>
  )
}
