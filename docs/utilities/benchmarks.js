const { parse } = require('csv-parse')
const fs = require('fs')
const path = require('path')

module.exports.process = async root => {
  const files = await new Promise((resolve, reject) => {
    fs.readdir(root, (error, files) => {
      if (error) {
        reject(error)
      }
      else {
        resolve(files
          .filter(name => name.endsWith('.csv'))
          .map(name => path.resolve(root, name)))
      }
    })
  })

  const rows = (await Promise.all(files.map(file => new Promise((resolve, reject) => {
    fs.createReadStream(file).pipe(parse({
      cast: (value, context) => {
        switch (context.column) {
          case 'duration':
          case 'iterations':
          case 'run':
            return +value

          default:
            return value
        }
      },
      columns: true
    }, (error, rows) => {
      if (error) {
        reject(error)
      }
      else {
        resolve(rows)
      }
    }))
  })))).flat()

  const result = {
    runtimes: []
  }

  for (const row of rows) {
    let runtime = result.runtimes.find(r => r.name === row.runtime)

    if (!runtime) {
      runtime = {
        name: row.runtime,
        libraries: []
      }

      result.runtimes.push(runtime)
    }

    let library = runtime.libraries.find(l => l.name === row.library)

    if (!library) {
      library = {
        name: row.library,
        results: []
      }

      runtime.libraries.push(library)
    }

    let group = library.results.find(r =>
      r.suite === row.suite &&
      r.component === row.component &&
      r.iterations === row.iterations
    )

    if (!group) {
      group = {
        component: row.component,
        iterations: row.iterations,
        suite: row.suite,
        times: []
      }

      library.results.push(group)
    }

    group.times.push(row.duration)
  }

  return result
}
