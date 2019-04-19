import { Link } from 'gatsby'
import React from 'react'

import ExternalLink from '../site/external-link'

import { join } from '../../../utilities/array'

import {
  createDocfxUrl,
  createMemberName,
  createTypeName,
  getMethodParameters,
  getTypeParameters,
  getTypeSuffix
} from '../../../utilities/dotnet'

const confluentDocfxBase = 'https://docs.confluent.io/current/clients/confluent-kafka-dotnet/api/'
const microsoftDocfxBase = 'https://docs.microsoft.com/en-us/dotnet/api/'

function DotnetExpansion ({ id, typeParameters = [], methodTypeParameters = [] }) {
  switch (id.substring(0, 2)) {
    case 'F:':
    case 'P:':
      return <DotnetLink id={id}>{createMemberName(id)}</DotnetLink>

    case 'M:':
      const method = [<DotnetLink key={id} id={id}>{createMemberName(id)}</DotnetLink>]

      if (methodTypeParameters && methodTypeParameters.length) {
        method.push('<', ...join(methodTypeParameters , ', '), '>')
      }

      method.push('(', ...join(getMethodParameters(id).map((type, index) =>
        <DotnetExpansion key={index} id={`T:${type}`} typeParameters={typeParameters} methodTypeParameters={methodTypeParameters} />
      ), ', '), ')');

      return method

    case 'T:':
      const typeBinding = id.match(/^T:`(\d+)$/)
      const methodBinding = id.match(/^T:``(\d+)$/)

      if (typeBinding) {
        return typeParameters[typeBinding[1]] || ''
      }
      else if (methodBinding) {
        return methodTypeParameters[methodBinding[1]] || ''
      }

      const type = [<DotnetLink key={id} id={id}>{createTypeName(id)}</DotnetLink>]
      const bound = getTypeParameters(id)
      const unbound = id.match(/^T:(.+)`(\d+)$/)

      if (bound || unbound) {
        type.push('<')

        if (bound) {
          type.push(...join(bound.map((type, index) =>
            <DotnetExpansion key={index} id={`T:${type}`} typeParameters={typeParameters} methodTypeParameters={methodTypeParameters} />
          ), ', '))
        }

        if (unbound) {
          id = unbound[1]
          type.push(new Array(+unbound[2]).join(','))
        }

        type.push('>')
      }

      type.push(getTypeSuffix(id))
      return type

    default:
      return <DotnetLink id={id}>{id.substring(2)}</DotnetLink>
  }
}

function DotnetLink ({ children, id }) {
  if (/^[EFMNPT]:Chr\.Avro/.test(id)) {
    return <Link to={`/api/${createDocfxUrl(id).toLowerCase()}`}>{children}</Link>
  }

  if (/^[EFMNPT]:Confluent\.(?:Kafka|SchemaRegistry)/.test(id)) {
    return <ExternalLink to={`${confluentDocfxBase}${createDocfxUrl(id)}.html`}>{children}</ExternalLink>
  }

  if (/^[EFMNPT]:(?:Microsoft|System)/.test(id)) {
    return <ExternalLink to={`${microsoftDocfxBase}${createDocfxUrl(id).toLowerCase()}`}>{children}</ExternalLink>
  }

  return children
}

export default function DotnetReference ({ children, id, typeParameters, methodTypeParameters, ...others }) {
  return children
    ? <DotnetLink id={id} {...others}>{children}</DotnetLink>
    : <code {...others}>
        <DotnetExpansion id={id} typeParameters={typeParameters} methodTypeParameters={methodTypeParameters} />
      </code>
}
