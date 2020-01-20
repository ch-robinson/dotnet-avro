import { Link, graphql, useStaticQuery } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../components/code/highlight'
import ExternalLink from '../components/site/external-link'

const title = 'CLI reference'

export default () => {
  const {
    allCliVerb: {
      nodes: cliVerbs
    },
    site: {
      siteMetadata: { latestRelease, projectName }
    }
  } = useStaticQuery(graphql`
    query {
      allCliVerb {
        nodes {
          id
          name
          summary
        }
      }
      site {
        siteMetadata {
          latestRelease
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
      <p>The {projectName} CLI is a <ExternalLink to='https://docs.microsoft.com/en-us/dotnet/core/tools'>.NET Core CLI tool</ExternalLink> that automates schema creation, registry operations, and more. To install:</p>
      <Highlight language='bash'>{`$ dotnet tool install Chr.Avro.Cli --global
Tool 'chr.avro.cli' (version '${latestRelease}') was successfully installed.`}</Highlight>

      <p>Once the CLI is installed, use <code>help</code> to see available commands and options:</p>
      <Highlight language='bash'>{`$ dotnet avro help
Chr.Avro.Cli ${latestRelease}
Copyright (C) 2019 C.H. Robinson
...`}</Highlight>

      <p>The same information is also available here:</p>
      <table>
        <thead>
          <tr>
            <th>Verb</th>
            <th>Summary</th>
          </tr>
        </thead>

        <tbody>
          {cliVerbs.map(cliVerb =>
            <tr key={cliVerb.id}>
              <td>
                <Link to={`/cli/${cliVerb.name}`}><code>{cliVerb.name}</code></Link>
              </td>
              <td>
                {cliVerb.summary}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </>
  )
}
