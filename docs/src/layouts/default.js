import { Link, StaticQuery, graphql, withPrefix } from 'gatsby'
import React, { Component } from 'react'
import { Helmet } from 'react-helmet'

import Hamburger from '../components/site/hamburger'
import Navigation from '../components/site/navigation'
import VersionStripe from '../components/site/version-stripe'

import * as styles from './default.module.scss'

class Layout extends Component {
  constructor (props) {
    super(props)

    this.state = {
      isNavigationExpanded: false
    }
  }

  toggleNavigation () {
    this.setState({
      isNavigationExpanded: !this.state.isNavigationExpanded
    })
  }

  render () {
    const { children, metadata } = this.props
    const { isNavigationExpanded } = this.state

    return (
      <>
        <Helmet defaultTitle={metadata.projectName} titleTemplate={`${metadata.projectName} ▸ %s`}>
          <link rel="icon" type="image/x-icon" href={withPrefix('/favicon.ico')} />
        </Helmet>

        <div className={styles.main}>
          <VersionStripe githubUrl={metadata.githubUrl} version={metadata.latestRelease} />

          <div className={styles.content}>
            {children}
          </div>
        </div>

        <div className={isNavigationExpanded ? styles.sidebarExpanded : styles.sidebarCollapsed}>
          <Link className={styles.brand} to='/'>
            {metadata.projectName}
          </Link>

          <Navigation className={styles.content} />

          <button className={styles.toggle} onClick={() => this.toggleNavigation()} tabIndex='0'>
            <Hamburger active={isNavigationExpanded}>Toggle navigation</Hamburger>
          </button>
        </div>
      </>
    )
  }
}

const siteDataQuery = graphql`
  query SiteDataQuery {
    site {
      siteMetadata {
        githubUrl
        latestRelease
        projectName
      }
    }
  }
`

export default function DefaultLayout (props) {
  return (
    <StaticQuery
      render={data => <Layout metadata={data.site.siteMetadata} {...props} />}
      query={siteDataQuery}
    />
  )
}
