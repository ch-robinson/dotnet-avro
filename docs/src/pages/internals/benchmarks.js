import { graphql, useStaticQuery } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import BenchmarksTable from '../../components/benchmarks/benchmarks-table'
import ExternalLink from '../../components/site/external-link'

const title = 'Performance and benchmarks'

export default function BenchmarksPage () {
  const {
    allBenchmarkSuite: { nodes: suites },
    site: {
      siteMetadata: { githubUrl, projectName }
    }
  } = useStaticQuery(graphql`
    query {
      allBenchmarkSuite {
        nodes {
          id
          name
          results {
            component
            iterations
            library {
              id
              name
            }
            times
          }
        }
      }
      site {
        siteMetadata {
          githubUrl
          projectName
        }
      }
    }
  `)

  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>

      <h1>{title}</h1>
      <p>To ensure that changes to {projectName} don’t introduce major performance regressions (and to highlight areas where performance could be improved), we maintain a <ExternalLink to={`${githubUrl}/tree/master/benchmarks`}>set of rudimentary benchmarks</ExternalLink> to test serialization throughput.</p>
      <p>Each benchmark runs five times, and we take the average time of those runs. These results were collected on a GitHub Actions worker running Ubuntu:</p>

      <BenchmarksTable suites={suites} />

      <p>In the future, we’re hoping to expand the benchmarks to test more complex situations as well as non-.NET libraries.</p>
    </>
  )
}
