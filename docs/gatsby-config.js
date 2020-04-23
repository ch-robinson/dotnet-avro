'use strict'

const path = require('path')

module.exports = {
  siteMetadata: {
    githubUrl: 'https://github.com/ch-robinson/dotnet-avro',
    latestRelease: '2.1.2',
    projectName: 'Chr.Avro'
  },
  pathPrefix: '/dotnet-avro',
  plugins: [
    {
      resolve: 'gatsby-plugin-layout',
      options: {
        component: path.resolve('./src/layouts/default.js')
      }
    },
    {
      resolve: 'gatsby-plugin-sass'
    },
    {
      resolve: 'local-api-reference',
      options: {
        path: path.resolve('./api'),
        memberComponent: path.resolve('./src/templates/dotnet-member.js'),
        namespaceComponent: path.resolve('./src/templates/dotnet-namespace.js'),
        typeComponent: path.resolve('./src/templates/dotnet-type.js')
      }
    },
    {
      resolve: 'local-benchmarks',
      options: {
        path: path.resolve('./benchmarks')
      }
    },
    {
      resolve: 'local-cli-reference',
      options: {
        path: path.resolve('./cli'),
        verbComponent: path.resolve('./src/templates/cli-verb.js')
      }
    }
  ]
}
