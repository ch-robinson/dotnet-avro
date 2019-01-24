import React, { Fragment } from 'react'

import DotnetReference from '../references/dotnet'

import MemberDetail from './member-detail'
import XmlDoc from './xml-doc'

import styles from './overload-table.module.scss'

export default ({ member }) =>
  <>
    <table className={styles.table}>
      <tbody>
        {member.overloads.map(overload =>
          <tr key={overload.id}>
            <th><DotnetReference id={overload.id} /></th>
            <td>{overload.summary && <XmlDoc xml={overload.summary} />}</td>
          </tr>
        )}
      </tbody>
    </table>

    {member.overloads.map(overload =>
      <Fragment key={overload.id}>
        <h3><DotnetReference id={overload.id} /></h3>
        <MemberDetail key={overload.id} member={{ ...member, ...overload }} />
      </Fragment>
    )}
  </>
