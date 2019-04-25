import { Link, StaticQuery, graphql } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../components/code/highlight'
import ExternalLink from '../components/site/external-link'

const title = 'CLI reference'

const CliReferencePage = ({ cliVerbs }) =>
  <>
    <Helmet>
      <title>{title}</title>
    </Helmet>

    <h1>{title}</h1>
    <p>The Chr.Avro CLI is a <ExternalLink to='https://docs.microsoft.com/en-us/dotnet/core/tools'>.NET Core CLI tool</ExternalLink> that automates schema creation, registry operations, and more. To install:</p>
    <Highlight language='shell'>{`$ dotnet tool install Chr.Avro.Cli --global
Tool 'chr.avro.cli' (version '1.0.0') was successfully installed.`}</Highlight>

    <p>Because Chr.Avro is still in pre-release, the version always has to be explicitly specified. Once the CLI is installed, use <code>help</code> to see available commands and options:</p>
    <Highlight language='shell'>{`$ dotnet avro help
Chr.Avro 1.0.0
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

const cliVerbQuery = graphql`
  query CliVerbQuery {
    allCliVerb {
      edges {
        node {
          id
          name
          summary
        }
      }
    }
  }
`

export default props =>
  <StaticQuery
    render={data => <CliReferencePage cliVerbs={data.allCliVerb.edges.map(e => e.node)} {...props} />}
    query={cliVerbQuery}
  />
