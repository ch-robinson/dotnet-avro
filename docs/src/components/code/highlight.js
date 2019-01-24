import React from 'react'
import { LightAsync as SyntaxHighlighter } from 'react-syntax-highlighter'

import csharp from 'react-syntax-highlighter/dist/languages/hljs/cs'
import json from 'react-syntax-highlighter/dist/languages/hljs/json'
import shell from 'react-syntax-highlighter/dist/languages/hljs/shell'

import github from 'react-syntax-highlighter/dist/styles/hljs/github'

SyntaxHighlighter.registerLanguage('avro', json)
SyntaxHighlighter.registerLanguage('csharp', csharp)
SyntaxHighlighter.registerLanguage('shell', shell)

// remove the default <pre> styling
const style = {
  ...github,
  hljs: {}
}

export default function Highlight ({ children, inline, language }) {
  return (
    <SyntaxHighlighter language={language} style={style} PreTag={inline ? 'span' : 'pre'}>
      {children}
    </SyntaxHighlighter>
  )
}
