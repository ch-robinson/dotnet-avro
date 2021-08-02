import React from 'react'

import ExternalLink from './external-link'

import * as styles from './version-stripe.module.scss'

export default function VersionStripe ({ githubUrl, version }) {
  return (
    <div className={styles.stripe}>
      <ExternalLink className={styles.version} to={`${githubUrl}/releases/tag/${version}`}>{version}</ExternalLink>
    </div>
  )
}
