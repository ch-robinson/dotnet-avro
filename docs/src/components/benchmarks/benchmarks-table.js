import React, { useMemo } from 'react'

import * as styles from './benchmarks-table.module.scss'

export default function BenchmarksTable ({ suites }) {
  const libraries = useMemo(() => {
    const map = new Map()

    for (const suite of suites) {
      for (const result of suite.results) {
        if (!map.has(result.library.id)) {
          map.set(result.library.id, result.library)
        }
      }
    }

    return Array.from(map.values())
      .sort((a, b) => a.name.localeCompare(b.name))
  }, [suites])

  const grid = useMemo(() => suites.map(suite => {
    const rows = []

    for (const result of suite.results) {
      let row = rows.find(r =>
        r.component === result.component &&
        r.iterations === result.iterations
      )

      if (!row) {
        row = {
          component: result.component,
          iterations: result.iterations,
          results: new Array(libraries.length).fill(null)
        }

        rows.push(row)
      }

      const index = libraries.findIndex(l => l.id === result.library.id)

      if (index > -1) {
        row.results[index] = {
          averageValue: result.times.reduce((a, b) => a + b) / result.times.length,
          values: result.times
        }
      }
    }

    return {
      rows: rows
        .map(row => ({
          ...row,
          component: row.component.toLowerCase(),
          iterations: row.iterations.toLocaleString({
            useGrouping: true
          })
        }))
        .sort((a, b) => a.component.localeCompare(b.component)),
      suite
    }
  }).sort((a, b) => a.suite.name.localeCompare(b.suite.name)), [libraries, suites])

  return (
    <table>
      <thead>
        <tr>
          <th colSpan={2} rowSpan={2}>Benchmark</th>
          <th colSpan={libraries.length}>Average time (ms)</th>
        </tr>
        <tr>
          {libraries.map(library =>
            <th className={styles.column} key={library.id}>{library.name}</th>
          )}
        </tr>
      </thead>

      <tbody>
        {grid.map(({ rows, suite }) => rows.map((row, index) =>
          <tr key={index}>
            {index === 0 &&
              <th className={styles.suite} rowSpan={rows.length}>
                {suite.name}
              </th>
            }

            <td className={styles.component}>
              <div>{row.component}</div>
              <div className={styles.iterationCount}>{row.iterations} iterations</div>
            </td>

            {row.results.map((result, index) =>
              <td className={result ? styles.result : styles.missing} key={index}>
                {result
                  ? result.averageValue.toFixed(3)
                  : 'n/a'
                }
              </td>
            )}
          </tr>
        ))}
      </tbody>
    </table>
  )
}
