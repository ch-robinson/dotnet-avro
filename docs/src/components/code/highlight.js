import React from 'react'
import { LightAsync as SyntaxHighlighter } from 'react-syntax-highlighter'

import bash from 'react-syntax-highlighter/dist/esm/languages/hljs/shell'
import csharp from 'react-syntax-highlighter/dist/esm/languages/hljs/cs'
import json from 'react-syntax-highlighter/dist/esm/languages/hljs/json'
import powershell from 'react-syntax-highlighter/dist/esm/languages/hljs/powershell'

import github from 'react-syntax-highlighter/dist/esm/styles/hljs/github'

SyntaxHighlighter.registerLanguage('avro', json)
SyntaxHighlighter.registerLanguage('bash', bash)
SyntaxHighlighter.registerLanguage('csharp', csharp)
SyntaxHighlighter.registerLanguage('powershell', powershell)

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
