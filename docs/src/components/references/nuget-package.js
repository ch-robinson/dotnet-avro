import React from 'react'

import ExternalLink from '../site/external-link'

export default function NugetPackageReference ({ children, id, version, ...others }) {
  let url = `https://www.nuget.org/packages/${id}`

  if (version) {
    url += `/${version}`
  }

  return <ExternalLink {...others} to={url}>{children || id}</ExternalLink>
}
