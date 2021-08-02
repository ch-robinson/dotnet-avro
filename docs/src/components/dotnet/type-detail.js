import React from 'react'

import InheritanceTable from './inheritance-table'
import MemberTable from './member-table'
import XmlDoc from './xml-doc'

import { groupBy } from '../../../utilities/array'

export default function TypeDetail ({ type }) {
  const { remarks, summary, typeParameters } = type

  const members = groupBy((type.members || []), 'kind')
  const constructors = members.get('constructor')
  const fields = members.get('field')
  const properties = members.get('property')
  const methods = members.get('method')

  return (
    <>
      {summary &&
        <XmlDoc xml={summary} />
      }

      <InheritanceTable type={type} />

      {remarks &&
        <XmlDoc xml={remarks} />
      }

      {constructors &&
        <>
          <h3>Constructors</h3>
          <MemberTable members={constructors} typeParameters={typeParameters} />
        </>
      }

      {fields &&
        <>
          <h3>Fields</h3>
          <MemberTable members={fields} typeParameters={typeParameters} />
        </>
      }

      {properties &&
        <>
          <h3>Properties</h3>
          <MemberTable members={properties} typeParameters={typeParameters} />
        </>
      }

      {methods &&
        <>
          <h3>Methods</h3>
          <MemberTable members={methods} typeParameters={typeParameters} />
        </>
      }
    </>
  )
}
