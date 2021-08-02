import React from 'react'

import * as styles from './breadcrumb.module.scss'

export default function Breadcrumb ({ children, ...others }) {
  if (!children) {
    return
  }

  if (!Array.isArray(children)) {
    children = [children]
  }

  return (
    <ul className={styles.breadcrumb} {...others}>
      {children.map((child, index) =>
        <li key={index}>{child}</li>
      )}
    </ul>
  )
}
