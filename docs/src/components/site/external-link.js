import React from 'react'

export default ({ children, to, ...others }) =>
  <a {...others} href={to} rel='noopener noreferrer' target='_blank'>{children}</a>
