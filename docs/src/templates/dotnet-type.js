import { Link, graphql } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import TypeDetail from '../components/dotnet/type-detail'
import DotnetReference from '../components/references/dotnet'
import Breadcrumb from '../components/site/breadcrumb'

import * as styles from './dotnet.module.scss'

export default function DotnetType ({ data: { dotnetType }}) {
  const { kind, name, namespace } = dotnetType

  return (
    <>
      <Helmet>
        <title>{`${name} ${kind}`}</title>
      </Helmet>

      <Breadcrumb>
        <Link to='/api'>API reference</Link>
        <DotnetReference id={namespace.id}>
          <code>{namespace.name}</code> namespace
        </DotnetReference>
      </Breadcrumb>

      <h1 className={styles.title}>
        <code>{name}</code> {kind}
      </h1>

      <TypeDetail type={dotnetType} />
    </>
  )
}

export const query = graphql`
  query DotnetTypeDetailQuery($id: String!) {
    dotnetType(id: { eq: $id }) {
      base
      kind
      interfaces
      name
      remarks
      summary
      assembly {
        id
      }
      members {
        id
        kind
        name
        overloads {
          id
          returns {
            summary
            type
          }
          summary
          typeParameters {
            name
          }
        }
      }
      namespace {
        id
        name
      }
      typeParameters {
        name
      }
      typeSignatures {
        language
        value
      }
    }
  }
`
