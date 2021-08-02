import { Link, graphql, useStaticQuery } from 'gatsby'
import React from 'react'

import { createDocfxUrl } from '../../../utilities/dotnet'

import * as styles from './navigation.module.scss'

export default function Navigation (props) {
  const {
    allCliVerb: {
      nodes: cliVerbs
    },
    allDotnetNamespace: {
      nodes: dotnetNamespaces
    }
  } = useStaticQuery(graphql`
    query ReferenceDataQuery {
      allCliVerb {
        nodes {
          id
          name
        }
      }
      allDotnetNamespace {
        nodes {
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
  `)

  return (
    <div {...props}>
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
          <Link to='/guides/extending'>
            Extending and overriding built-in features
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
                  <Link className={styles[`${dotnetType.kind}Link`]} to={`/api/${createDocfxUrl(dotnetType.id).toLowerCase()}`}>
                    {dotnetType.name}
                  </Link>
                </li>
              )}
            </ul>
          </li>
        )}
      </ul>
    </div>
  )
}
