import { Link, graphql } from 'gatsby'
import React, { Fragment } from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../components/code/highlight'
import Breadcrumb from '../components/site/breadcrumb'

import { groupBy } from '../../utilities/array'

import styles from './cli.module.scss'

export default ({ data: { cliVerb } }) =>
  <>
    <Helmet>
      <title>{`dotnet avro ${cliVerb.name}`}</title>
    </Helmet>

    <Breadcrumb>
      <Link to='/cli'>CLI reference</Link>
    </Breadcrumb>

    <h1 className={styles.title}>
      <code>{cliVerb.name}</code>
    </h1>

    <p>{cliVerb.summary}</p>

    {cliVerb.examples.length > 0 &&
      <>
        <h3>Examples</h3>

        {cliVerb.examples.map(example =>
          <Fragment key={example.id}>
            <h5>{example.title}</h5>
            <Highlight language={example.language}>{example.body}</Highlight>
          </Fragment>
        )}
      </>
    }

    {cliVerb.options.length > 0 &&
      <>
        <h3>Options</h3>
        {Array.from(groupBy(cliVerb.options, 'set')).map((set, index) =>
          <Fragment key={index}>
            {set[0] &&
              <h5>{set[0]}</h5>
            }

            {set[1].length > 0 &&
              <dl>
                {set[1].map(option =>
                  <Fragment key={option.id}>
                    <dt>
                      <code>
                        {option.abbreviation && `-${option.abbreviation}, `}
                        {`--${option.name}`}
                      </code>
                    </dt>
                    <dd>
                      {option.summary}
                    </dd>
                  </Fragment>
                )}
              </dl>
            }
          </Fragment>
        )}
      </>
    }
  </>

export const query = graphql`
  query CliVerbDetailQuery($id: String!) {
    cliVerb(id: { eq: $id }) {
      name
      summary
      examples {
        id
        body
        language
        title
      }
      options {
        id
        abbreviation
        name
        required
        set
        summary
      }
    }
  }
`
