import React from 'react'

import ExternalLink from './external-link'

import styles from './version-stripe.module.scss'

export default ({ githubUrl, version }) =>
  <div className={styles.stripe}>
    <ExternalLink className={styles.version} to={`${githubUrl}/releases/tag/${version}`}>{version}</ExternalLink>
  </div>
