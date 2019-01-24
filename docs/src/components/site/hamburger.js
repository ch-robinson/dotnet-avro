import React from 'react'

import styles from './hamburger.module.scss'

export default ({ active, children, ...others }) =>
  <span className={active ? styles.hamburgerActive : styles.hamburgerInactive} {...others}>
    {children}
  </span>
