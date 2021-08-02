import React from 'react'
import { Helmet } from 'react-helmet'

const title = 'API reference'

export default function ApiPage () {
  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>

      <h1>{title}</h1>
    </>
  )
}
