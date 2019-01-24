import React from 'react'

import styles from './breadcrumb.module.scss'

export default ({ children, ...others }) => {
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
