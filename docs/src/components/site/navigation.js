import { Link, StaticQuery, graphql } from 'gatsby'
import React from 'react'

import { createDocfxUrl } from '../../../utilities/dotnet'

import styles from './navigation.module.scss'

const Navigation = ({ cliVerbs, dotnetNamespaces, ...others }) =>
  <div {...others}>
    <h4 className={styles.heading}>Guides</h4>
    <ul className={styles.links}>
      <li>
        <Link to='/guides/kafka'>
          Building Kafka producers and consumers
        </Link>
      </li>

      <li>
        <Link to='/guides/cli-create'>
          Creating schemas from .NET types
        </Link>
      </li>

      <li>
        <Link to='/guides/cli-generate'>
          Generating C# code from Avro schemas
        </Link>
      </li>
    </ul>

    <h4 className={styles.heading}>Technical documentation</h4>
    <ul className={styles.links}>
      <li>
        <Link to='/internals/implementation'>
          Implementation differences
        </Link>
      </li>

      <li>
        <Link to='/internals/benchmarks'>
          Performance and benchmarks
        </Link>
      </li>

      <li>
        <Link to='/internals/schema-compatibility'>
          Schema compatibility
        </Link>
      </li>

      <li>
        <Link to='/internals/mapping'>
          Types and conversions
        </Link>
      </li>
    </ul>

    <h4 className={styles.heading}>
      <Link to='/cli'>
        CLI reference
      </Link>
    </h4>
    <ul className={styles.links}>
      {cliVerbs.map(cliVerb =>
        <li key={cliVerb.id}>
          <Link to={`/cli/${cliVerb.name}`}>
            {cliVerb.name}
          </Link>
        </li>
      )}
    </ul>

    <h4 className={styles.heading}>
      <Link to='/api'>
        API reference
      </Link>
    </h4>
    <ul className={styles.links}>
      {dotnetNamespaces.map(dotnetNamespace =>
        <li key={dotnetNamespace.id}>
          <Link to={`/api/${createDocfxUrl(dotnetNamespace.id).toLowerCase()}`}>
            {dotnetNamespace.name}
          </Link>

          <ul className={styles.links}>
            {dotnetNamespace.types.map(dotnetType =>
              <li key={dotnetType.id}>
                <Link className={styles[dotnetType.kind]} to={`/api/${createDocfxUrl(dotnetType.id).toLowerCase()}`}>
                  {dotnetType.name}
                </Link>
              </li>
            )}
          </ul>
        </li>
      )}
    </ul>
  </div>

const referenceDataQuery = graphql`
  query ReferenceDataQuery {
    allCliVerb {
      edges {
        node {
          id
          name
        }
      }
    }
    allDotnetNamespace {
      edges {
        node {
          id
          name
          types {
            id
            kind
            name
          }
        }
      }
    }
  }
`

export default props =>
  <StaticQuery
    render={data => <Navigation
      cliVerbs={data.allCliVerb.edges.map(e => e.node)}
      dotnetNamespaces={data.allDotnetNamespace.edges.map(e => e.node)}
      {...props}
    />}
    query={referenceDataQuery}
  />
