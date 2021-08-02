import React from 'react'

export default function ExternalLink ({ children, to, ...others }) {
  return (
    <a {...others} href={to} rel='noopener noreferrer' target='_blank'>{children}</a>
  )
}
