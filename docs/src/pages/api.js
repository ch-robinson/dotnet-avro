import { StaticQuery, graphql } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

const title = 'API reference'

const ApiReferencePage = ({ dotnetNamespaces }) =>
  <>
    <Helmet>
      <title>{title}</title>
    </Helmet>

    <h1>{title}</h1>
  </>

const cliVerbQuery = graphql`
  query DotnetNamespaceQuery {
    allDotnetNamespace {
      edges {
        node {
          id
          name
        }
      }
    }
  }
`

export default props =>
  <StaticQuery
    render={data => <ApiReferencePage
      dotnetNamespaces={data.allDotnetNamespace.edges.map(e => e.node)}
      {...props}
    />}
    query={cliVerbQuery}
  />
