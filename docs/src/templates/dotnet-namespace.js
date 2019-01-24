import { Link, graphql } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import NamespaceDetail from '../components/dotnet/namespace-detail'
import Breadcrumb from '../components/site/breadcrumb'

import styles from './dotnet.module.scss'

export default ({ data: { dotnetNamespace }}) =>
  <>
    <Helmet>
      <title>{`${dotnetNamespace.name} namespace`}</title>
    </Helmet>

    <Breadcrumb>
      <Link to='/api'>API reference</Link>
    </Breadcrumb>

    <h1 className={styles.title}>
      <code>{dotnetNamespace.name}</code> namespace
    </h1>

    <NamespaceDetail namespace={dotnetNamespace} />
  </>

export const query = graphql`
  query DotnetNamespaceDetailQuery($id: String!) {
    dotnetNamespace(id: { eq: $id }) {
      id
      name
      types {
        id
        kind
        name
        namespace {
          id
        }
        summary
      }
    }
  }
`
