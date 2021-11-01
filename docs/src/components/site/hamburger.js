import React from 'react'

import * as styles from './hamburger.module.scss'

export default function Hamburger ({ active, children, ...others }) {
  return (
    <span className={active ? styles.hamburgerActive : styles.hamburgerInactive} {...others}>
      {children}
    </span>
  )
}
