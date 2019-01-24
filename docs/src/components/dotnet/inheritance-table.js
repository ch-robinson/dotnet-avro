import React from 'react'

import Highlight from '../code/highlight'
import DotnetReference from '../references/dotnet'

import styles from './inheritance-table.module.scss'

export default ({ member, type }) => {
  const object = type || member

  const { assembly, base, interfaces, namespace } = object
  const signature = (object.typeSignatures || object.memberSignatures || []).find(s => s.language === 'C#')
  const typeParameters = (object.typeParameters || []).map(p => p.name)

  return (
    <table className={styles.table}>
      <tbody>
        {assembly &&
          <tr>
            <th>Assembly</th>
            <td>
              <DotnetReference id={assembly.id} />
            </td>
          </tr>
        }

        {namespace &&
          <tr>
            <th>Namespace</th>
            <td>
              <DotnetReference id={namespace.id} />
            </td>
          </tr>
        }

        {signature &&
          <tr>
            <th>Signature</th>
            <td>
              <Highlight language='csharp'>{signature.value}</Highlight>
            </td>
          </tr>
        }

        {base &&
          <tr>
            <th>Base</th>
            <td>
              <DotnetReference id={base} typeParameters={typeParameters} />
            </td>
          </tr>
        }

        {interfaces && interfaces.length > 0 && interfaces.map((id, index) =>
          <tr key={index}>
            {index === 0 &&
              <th rowSpan={interfaces.length}>Interfaces</th>
            }
            <td>
              <DotnetReference id={id} typeParameters={typeParameters} />
            </td>
          </tr>
        )}
      </tbody>
    </table>
  )
}
