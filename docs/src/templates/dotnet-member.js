import { Link, graphql } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import MemberDetail from '../components/dotnet/member-detail'
import OverloadTable from '../components/dotnet/overload-table'
import DotnetReference from '../components/references/dotnet'
import Breadcrumb from '../components/site/breadcrumb'

import * as styles from './dotnet.module.scss'

export default function DotnetMember ({ data: { dotnetMember }}) {
  const { kind, name, overloads, type } = dotnetMember

  return (
    <>
      <Helmet>
        <title>{`${kind === 'constructor' ? type.name : name} ${kind}`}</title>
      </Helmet>

      <Breadcrumb>
        <Link to='/api'>API reference</Link>
        <DotnetReference id={type.namespace.id}>
          <code>{type.namespace.name}</code> namespace
        </DotnetReference>
        <DotnetReference id={type.id}>
          <code>{type.name}</code> {type.kind}
        </DotnetReference>
      </Breadcrumb>

      <h1 className={styles.title}>
        <code>{kind === 'constructor' ? type.name : name}</code> {kind}
      </h1>

      {overloads.length === 1
        ? <MemberDetail member={{ ...dotnetMember, ...overloads[0] }} />
        : <OverloadTable member={dotnetMember} />
      }
    </>
  )
}

export const query = graphql`
  query DotnetMemberDetailQuery($id: String!) {
    dotnetMember(id: { eq: $id }) {
      id
      kind
      name
      overloads {
        id
        base
        exceptions {
          summary
          type
        }
        memberSignatures {
          language
          value
        }
        parameters {
          name
          summary
          type
        }
        remarks
        returns {
          summary
          type
        }
        summary
        typeParameters {
          name
          summary
        }
      }
      type {
        id
        kind
        name
        namespace {
          id
          name
        }
        typeParameters {
          name
        }
      }
    }
  }
`
