'use strict'

module.exports.onCreateWebpackConfig = function ({ actions }) {
  // fix errors from event-source-polyfill
  actions.setWebpackConfig({
    resolve: {
      fallback: {
        http: false,
        https: false
      }
    }
  })
}
