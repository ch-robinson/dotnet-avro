import React from 'react'

import ExternalLink from '../components/site/external-link'

import * as styles from './index.module.scss'

export default function IndexPage () {
  return (
    <>
      <div className={styles.hero}>
        Chr.Avro is an Avro implementation for .NET.
      </div>

      <p>
        It’s designed to serve as a flexible alternative to the <ExternalLink to='https://github.com/apache/avro/tree/master/lang/csharp/src/apache/main'>Apache Avro implementation</ExternalLink> and integrate seamlessly with <ExternalLink to='https://github.com/confluentinc/confluent-kafka-dotnet' target='_blank'>Confluent’s Kafka and Schema Registry clients</ExternalLink>.
      </p>
    </>
  )
}
