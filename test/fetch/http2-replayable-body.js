'use strict'

const { test } = require('node:test')
const { tspl } = require('@matteo.collina/tspl')
const { constants, createSecureServer } = require('node:http2')
const { once } = require('node:events')
const { Readable } = require('node:stream')

const pem = require('@metcoder95/https-pem')

const { fetch, Client, Agent, FormData } = require('../..')
const { closeClientAndServerAsPromise } = require('../utils/node-http')

const { ReplayableBody } = require('../../lib/web/fetch/replayable-body')
const { kBodyReplayable } = require('../../lib/core/symbols')
async function createServerAndClient (t, onStream, onSession, dispatcherFactory = (origin) => new Client(origin, {
  connect: { rejectUnauthorized: false },
  allowH2: true
})) {
  const server = createSecureServer(await pem.generate({ opts: { keySize: 2048 } }))
  server.on('stream', (stream, ...args) => {
    // Sink H2 reset/errors that fire after client aborts or GOAWAY frames,
    // otherwise they escape as uncaught exceptions in the test process.
    stream.on('error', () => {})
    onStream(stream, ...args)
  })
  server.on('session', (session, ...args) => {
    session.on('error', () => {})
    onSession?.(session, ...args)
  })

  server.listen(0)
  await once(server, 'listening')

  const dispatcher = dispatcherFactory(`https://localhost:${server.address().port}`)

  t.after(closeClientAndServerAsPromise(dispatcher, server))

  return { dispatcher, origin: `https://localhost:${server.address().port}` }
}

function collectRequest (stream, cb) {
  const chunks = []
  stream.on('data', chunk => chunks.push(chunk))
  stream.on('end', () => cb(Buffer.concat(chunks)))
}

function h2AgentFactory () {
  return new Agent({
    connect: { rejectUnauthorized: false },
    allowH2: true
  })
}

test('abandoning ReplayableBody iteration cancels its active stream', async (t) => {
  const p = tspl(t, { plan: 2 })
  let cancelled = false
  const stream = new ReadableStream({
    pull (controller) {
      controller.enqueue(new Uint8Array([1]))
    },
    cancel () {
      cancelled = true
    }
  })
  const body = new ReplayableBody(stream, null, () => {
    throw new Error('replay should not start')
  }, {
    isCancelled: () => false
  })
  const iterator = body[Symbol.asyncIterator]()

  await iterator.next()
  await iterator.return()

  p.strictEqual(cancelled, true)
  p.strictEqual(stream.locked, false)

  await p.completed
})

test('source-backed fetch POST bodies replay across GOAWAY with identical bytes and content-length', async (t) => {
  const p = tspl(t, { plan: 15 })

  const bodies = [
    { label: 'string', body: 'plain string body', type: 'text/plain' },
    { label: 'json', body: JSON.stringify({ foo: 'bar', count: 42 }), type: 'application/json' },
    { label: 'buffer', body: Buffer.from([0x00, 0x01, 0x02, 0x03, 0xff]), type: 'application/octet-stream' }
  ]

  let connectionIndex = 0
  let testStreamsOnFirst = 0
  let warmupStreamId = null
  const attempts = [[], []]
  const firstContentLengths = new Map()
  const sessionToIndex = new WeakMap()

  const { dispatcher, origin } = await createServerAndClient(t, (stream, headers) => {
    const idx = sessionToIndex.get(stream.session)

    if (headers[':path'] === '/warmup') {
      warmupStreamId = stream.id
      collectRequest(stream, (body) => {
        stream.respond({ ':status': 200 })
        stream.end(body || 'warmup')
      })
      return
    }

    // Record the full content-length from the first session even if the body
    // is interrupted by the GOAWAY; the replayed request carries the same value.
    if (idx === 0) {
      firstContentLengths.set(headers[':path'], headers['content-length'])
    }

    collectRequest(stream, (body) => {
      attempts[idx].push({
        path: headers[':path'],
        contentLength: headers['content-length'],
        body,
        streamId: stream.id
      })
      if (idx > 0) {
        stream.respond({ ':status': 200 })
        stream.end(body)
      }
    })

    // Accept only the warmup stream on the first session; replay the test
    // streams on a fresh connection.
    if (idx === 0 && warmupStreamId != null) {
      testStreamsOnFirst++
      if (testStreamsOnFirst === bodies.length) {
        stream.session.goaway(constants.NGHTTP2_NO_ERROR, warmupStreamId)
      }
    }
  }, (session) => {
    sessionToIndex.set(session, connectionIndex++)
  }, h2AgentFactory)

  // Warm up the first session with an accepted request.
  const warmup = await fetch(`${origin}/warmup`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body: 'warmup'
  })
  p.strictEqual(warmup.status, 200)
  p.strictEqual(await warmup.text(), 'warmup')

  // Issue the test requests on the same session; the server will GOAWAY after
  // seeing all three, with lastStreamID set to the accepted warmup stream.
  const responses = await Promise.all(bodies.map(({ body, type }, i) =>
    fetch(`${origin}/req-${i}`, {
      method: 'POST',
      dispatcher,
      headers: { 'content-type': type },
      body
    })
  ))

  const buffers = await Promise.all(responses.map(r => r.arrayBuffer()))

  for (let i = 0; i < bodies.length; i++) {
    const expected = typeof bodies[i].body === 'string'
      ? bodies[i].body
      : Buffer.from(bodies[i].body).toString('hex')
    const actual = typeof bodies[i].body === 'string'
      ? Buffer.from(buffers[i]).toString()
      : Buffer.from(buffers[i]).toString('hex')
    p.strictEqual(actual, expected, `body ${i} echoed unchanged`)
    p.strictEqual(responses[i].status, 200, `body ${i} status`)
  }

  // All replayed bodies were received on the second session.
  p.strictEqual(attempts[1].length, 3, 'all test bodies replayed on second session')

  // The replayed bytes and content-length match the originals.
  for (let i = 0; i < bodies.length; i++) {
    const replayed = attempts[1].find(a => a.path === `/req-${i}`)
    const expectedLength = firstContentLengths.get(`/req-${i}`)
    p.ok(replayed, `replayed attempt for req-${i}`)
    p.strictEqual(replayed.contentLength, expectedLength, `req-${i} content-length preserved across replay`)
  }

  await p.completed
})

test('FormData replay preserves the original serialized snapshot', async (t) => {
  const p = tspl(t, { plan: 8 })

  let connectionIndex = 0
  let warmupStreamId = null
  let targetStreamsSeen = 0
  let replayBody = null
  const contentTypes = []
  const sessionToIndex = new WeakMap()

  const { dispatcher, origin } = await createServerAndClient(t, (stream, headers) => {
    const idx = sessionToIndex.get(stream.session)

    if (headers[':path'] === '/warmup') {
      warmupStreamId = stream.id
      collectRequest(stream, () => {
        stream.respond({ ':status': 200 })
        stream.end('warmup')
      })
      return
    }

    targetStreamsSeen++
    contentTypes.push(headers['content-type'])

    if (idx === 0) {
      stream.on('data', () => {})
      stream.on('end', () => {})
      stream.session.goaway(constants.NGHTTP2_NO_ERROR, warmupStreamId)
      return
    }

    collectRequest(stream, (body) => {
      replayBody = body.toString()
      stream.respond({ ':status': 200 })
      stream.end('ok')
    })
  }, (session) => {
    sessionToIndex.set(session, connectionIndex++)
  }, h2AgentFactory)

  const warmup = await fetch(`${origin}/warmup`, { dispatcher })
  p.strictEqual(warmup.status, 200)

  const formData = new FormData()
  formData.append('before', 'initial')

  const responsePromise = fetch(`${origin}/target`, {
    method: 'POST',
    dispatcher,
    body: formData
  })
  formData.append('after', 'mutated')

  const response = await responsePromise
  p.strictEqual(response.status, 200)
  p.strictEqual(await response.text(), 'ok')
  p.strictEqual(targetStreamsSeen, 2, 'one refused stream plus one replay')
  p.strictEqual(contentTypes[1], contentTypes[0], 'multipart boundary is unchanged')
  p.ok(replayBody.includes('name="before"\r\n\r\ninitial'), 'original field was replayed')
  p.ok(!replayBody.includes('name="after"'), 'post-dispatch mutation was not replayed')

  const boundary = contentTypes[1].match(/boundary=(.+)$/)?.[1]
  p.ok(boundary != null && replayBody.startsWith(`--${boundary}\r\n`), 'body uses the retained boundary')

  await p.completed
})

test('source-backed fetch bodies respect the GOAWAY replay budget', async (t) => {
  const p = tspl(t, { plan: 6 })

  let connectionIndex = 0
  let sessionsCount = 0
  let warmupStreamId = null
  const targetAttempts = []
  const sessionToIndex = new WeakMap()
  const testStreamsSeen = new WeakMap()

  const { dispatcher, origin } = await createServerAndClient(t, (stream, headers) => {
    const idx = sessionToIndex.get(stream.session)
    const path = headers[':path']

    if (path === '/warmup') {
      warmupStreamId = stream.id
      collectRequest(stream, (body) => {
        stream.respond({ ':status': 200 })
        stream.end(body || 'warmup')
      })
      return
    }

    collectRequest(stream, (body) => {
      if (path === '/control') {
        // Accepted stream on session1; respond normally.
        stream.respond({ ':status': 200 })
        stream.end(body)
      }
      // /target gets no response on session1; the request-level budget rejects
      // after the second refusal.
    })

    if (path === '/target') {
      targetAttempts.push(idx)
    }

    // After both /control and /target headers have been seen, GOAWAY with
    // lastStreamID set to the accepted warmup stream. Both test streams are
    // unaccepted on session0 and requeue; on session1 control (id1) is accepted
    // while target (id3) is refused a second time and must reject.
    const seen = (testStreamsSeen.get(stream.session) ?? 0) + 1
    testStreamsSeen.set(stream.session, seen)
    if (seen === 2 && warmupStreamId != null) {
      stream.session.goaway(constants.NGHTTP2_NO_ERROR, warmupStreamId)
    }
  }, (session) => {
    sessionToIndex.set(session, connectionIndex++)
    sessionsCount++
  }, h2AgentFactory)

  // Warm up session0 with an accepted request.
  const warmup = await fetch(`${origin}/warmup`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body: 'warmup'
  })
  p.strictEqual(warmup.status, 200)

  const controlPromise = fetch(`${origin}/control`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body: 'control-body'
  })

  const targetPromise = fetch(`${origin}/target`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body: 'target-body'
  })

  const control = await controlPromise
  p.strictEqual(control.status, 200)
  p.strictEqual(await control.text(), 'control-body')

  await p.rejects(targetPromise)
  p.deepStrictEqual(targetAttempts, [0, 1], 'target refused on sessions 0 and 1, then budget exhausted')
  p.strictEqual(sessionsCount, 2, 'no third session created after budget exhausted')

  await p.completed
})

test('accepted stream IDs are not replayed after GOAWAY', async (t) => {
  const p = tspl(t, { plan: 5 })

  let connectionIndex = 0
  const attempts = []
  const sessionToIndex = new WeakMap()

  const { dispatcher, origin } = await createServerAndClient(t, (stream, headers) => {
    const idx = sessionToIndex.get(stream.session)
    attempts.push(idx)
    collectRequest(stream, (body) => {
      stream.respond({ ':status': 200 })
      stream.end(body)
      // GOAWAY with lastStreamID=3, sent after the response, declares streams
      // 1 and 3 accepted. Both must be allowed to complete instead of replayed.
      if (stream.id === 3) {
        stream.session.goaway(constants.NGHTTP2_NO_ERROR, 3)
      }
    })
  }, (session) => {
    sessionToIndex.set(session, connectionIndex++)
  }, h2AgentFactory)

  const first = fetch(`${origin}/first`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body: 'first'
  })

  const second = fetch(`${origin}/second`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body: 'second'
  })

  const [firstResponse, secondResponse] = await Promise.all([first, second])

  p.strictEqual(firstResponse.status, 200)
  p.strictEqual(await firstResponse.text(), 'first')
  p.strictEqual(secondResponse.status, 200)
  p.strictEqual(await secondResponse.text(), 'second')
  p.deepStrictEqual(attempts, [0, 0], 'both accepted streams completed on the first session')

  await p.completed
})

test('source-backed fetch bodies replay across RST_STREAM REFUSED_STREAM', async (t) => {
  const p = tspl(t, { plan: 3 })

  let connectionIndex = 0
  const attempts = []
  const sessionToIndex = new WeakMap()
  const refusedSession = new WeakSet()

  // REFUSED_STREAM retry is handled on the same live H2 session, so use a
  // direct Client rather than an Agent/Pool.
  const { dispatcher, origin } = await createServerAndClient(t, (stream, headers) => {
    const idx = sessionToIndex.get(stream.session)
    attempts.push(idx)
    collectRequest(stream, (body) => {
      stream.respond({ ':status': 200 })
      stream.end(body)
    })
    // Refuse the first stream on the session; the replay is issued on the same
    // live session and must succeed.
    if (!refusedSession.has(stream.session)) {
      refusedSession.add(stream.session)
      stream.close(constants.NGHTTP2_REFUSED_STREAM)
    }
  }, (session) => {
    sessionToIndex.set(session, connectionIndex++)
  })

  const response = await fetch(`${origin}/`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'application/octet-stream' },
    body: Buffer.from([0xde, 0xad, 0xbe, 0xef])
  })

  p.strictEqual(response.status, 200)
  p.deepStrictEqual(Buffer.from(await response.arrayBuffer()), Buffer.from([0xde, 0xad, 0xbe, 0xef]))
  p.deepStrictEqual(attempts, [0, 0], 'one refused-stream plus one replay on the same session')

  await p.completed
})
test('GOAWAY cancels an idle body writer before replaying', async (t) => {
  const p = tspl(t, { plan: 7 })

  let connectionIndex = 0
  let warmupStreamId = null
  let iteratorAttempts = 0
  let firstIteratorReturned = false
  let replayedBody = null
  const targetAttempts = []
  const sessionToIndex = new WeakMap()

  const body = {
    [kBodyReplayable]: true,
    [Symbol.asyncIterator] () {
      const attempt = iteratorAttempts++
      if (attempt === 0) {
        let resolveNext
        return {
          next () {
            return new Promise(resolve => {
              resolveNext = resolve
            })
          },
          return () {
            firstIteratorReturned = true
            resolveNext?.({ done: true, value: undefined })
            return { done: true, value: undefined }
          }
        }
      }

      let sent = false
      return {
        next () {
          if (sent) {
            return { done: true, value: undefined }
          }
          sent = true
          return { done: false, value: Buffer.from('body') }
        },
        return () {
          return { done: true, value: undefined }
        }
      }
    }
  }

  const { dispatcher, origin } = await createServerAndClient(t, (stream, headers) => {
    const idx = sessionToIndex.get(stream.session)

    if (headers[':path'] === '/warmup') {
      warmupStreamId = stream.id
      stream.respond({ ':status': 200 })
      stream.end('warmup')
      return
    }

    targetAttempts.push(idx)
    if (idx === 0) {
      stream.on('data', () => {})
      stream.on('end', () => {})
      stream.session.goaway(constants.NGHTTP2_NO_ERROR, warmupStreamId)
      return
    }

    collectRequest(stream, (bytes) => {
      replayedBody = bytes.toString()
      stream.respond({ ':status': 200 })
      stream.end('ok')
    })
  }, (session) => {
    sessionToIndex.set(session, connectionIndex++)
  }, h2AgentFactory)

  const warmup = await dispatcher.request({ origin, path: '/warmup', method: 'GET' })
  p.strictEqual(warmup.statusCode, 200)
  await warmup.body.text()

  const response = await dispatcher.request({
    origin,
    path: '/target',
    method: 'POST',
    headers: { 'content-length': '4' },
    body,
    signal: AbortSignal.timeout(3000)
  })

  p.strictEqual(response.statusCode, 200)
  p.strictEqual(await response.body.text(), 'ok')
  p.strictEqual(firstIteratorReturned, true)
  p.strictEqual(iteratorAttempts, 2)
  p.deepStrictEqual(targetAttempts, [0, 1])
  p.strictEqual(replayedBody, 'body')

  await p.completed
})
test('normally exhausted request bodies do not receive return', async (t) => {
  const p = tspl(t, { plan: 4 })
  let nextCalls = 0
  let returnCalls = 0
  const body = {
    [Symbol.asyncIterator] () {
      return {
        next () {
          nextCalls++
          return nextCalls === 1
            ? { done: false, value: Buffer.from('body') }
            : { done: true, value: undefined }
        },
        return () {
          returnCalls++
          return { done: true, value: undefined }
        }
      }
    }
  }

  const { dispatcher } = await createServerAndClient(t, (stream) => {
    collectRequest(stream, (bytes) => {
      stream.respond({ ':status': 200 })
      stream.end(bytes)
    })
  })

  const response = await dispatcher.request({
    path: '/',
    method: 'POST',
    headers: { 'content-length': '4' },
    body
  })

  p.strictEqual(response.statusCode, 200)
  p.strictEqual(await response.body.text(), 'body')
  p.strictEqual(nextCalls, 2)
  p.strictEqual(returnCalls, 0)

  await p.completed
})

test('abandons the first writer before replaying a large source-backed body after GOAWAY', async (t) => {
  const p = tspl(t, { plan: 8 })

  const source = Buffer.alloc(8 * 1024 * 1024, 0x61)
  const streamStats = []
  class TrackingBlob extends Blob {
    stream () {
      const streamIndex = streamStats.length
      const stats = { bytes: 0, reads: 0 }
      let offset = 0
      streamStats.push(stats)

      return new ReadableStream({
        async pull (controller) {
          const readIndex = stats.reads++
          if (streamIndex === 0 && readIndex > 0) {
            await new Promise(resolve => setTimeout(resolve, 100))
          }

          if (offset === source.byteLength) {
            controller.close()
            return
          }

          const end = Math.min(offset + 64 * 1024, source.byteLength)
          const chunk = source.subarray(offset, end)
          offset = end
          stats.bytes += chunk.byteLength
          controller.enqueue(chunk)
        }
      })
    }
  }
  const body = new TrackingBlob([source])
  let connectionIndex = 0
  let warmupStreamId = null
  let targetStreamsSeen = 0
  let replayBytes = 0
  let replayBytesIntact = true
  let replayContentLength = null
  const sessionToIndex = new WeakMap()

  const { dispatcher, origin } = await createServerAndClient(t, (stream, headers) => {
    const path = headers[':path']
    const idx = sessionToIndex.get(stream.session)

    if (path === '/warmup') {
      warmupStreamId = stream.id
      collectRequest(stream, (body) => {
        stream.respond({ ':status': 200 })
        stream.end(body || 'warmup')
      })
      return
    }

    targetStreamsSeen++
    if (idx === 0) {
      // Leave the first upload backpressured when GOAWAY detaches its stream.
      // The abandoned writer must not continue consuming or abort the replay.
      stream.pause()
      stream.session.goaway(constants.NGHTTP2_NO_ERROR, warmupStreamId)
      return
    }

    replayContentLength = headers['content-length']
    stream.on('data', (chunk) => {
      replayBytes += chunk.byteLength
      replayBytesIntact &&= chunk.every(byte => byte === 0x61)
    })
    stream.on('end', () => {
      setTimeout(() => {
        stream.respond({ ':status': 200 })
        stream.end('ok')
      }, 250)
    })
  }, (session) => {
    sessionToIndex.set(session, connectionIndex++)
  }, h2AgentFactory)

  const warmup = await fetch(`${origin}/warmup`, {
    method: 'POST',
    dispatcher,
    body: 'warmup'
  })
  p.strictEqual(warmup.status, 200)

  const response = await fetch(`${origin}/target`, {
    method: 'POST',
    dispatcher,
    body
  })

  p.strictEqual(response.status, 200)
  p.strictEqual(await response.text(), 'ok')
  p.strictEqual(targetStreamsSeen, 2, 'one abandoned stream plus one replay')
  p.strictEqual(replayContentLength, `${source.byteLength}`)
  p.ok(replayBytesIntact && replayBytes === source.byteLength, 'replayed bytes are complete and unchanged')
  p.strictEqual(streamStats.length, 2, 'one source stream per transmission attempt')
  p.ok(streamStats[0].bytes < source.byteLength, 'abandoned source stream was not fully consumed')

  await p.completed
})

test('source-null streaming fetch bodies are not replayed after GOAWAY', async (t) => {
  const p = tspl(t, { plan: 3 })

  let warmupStreamId = null
  let targetStreamsSeen = 0

  const { dispatcher, origin } = await createServerAndClient(t, (stream, headers) => {
    const path = headers[':path']

    if (path === '/warmup') {
      warmupStreamId = stream.id
      collectRequest(stream, (body) => {
        stream.respond({ ':status': 200 })
        stream.end(body || 'warmup')
      })
      return
    }

    targetStreamsSeen++
    stream.on('data', () => {})
    stream.on('end', () => {})
    // The target stream is unaccepted (id > warmup id). For a source-null body
    // it must not be replayed.
    if (warmupStreamId != null) {
      stream.session.goaway(constants.NGHTTP2_NO_ERROR, warmupStreamId)
    }
  }, undefined, h2AgentFactory)

  const warmup = await fetch(`${origin}/warmup`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body: 'warmup'
  })
  p.strictEqual(warmup.status, 200)

  const body = new Readable({
    read () {
      this.push('streaming-body')
      this.push(null)
    }
  })

  await p.rejects(fetch(`${origin}/target`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body,
    // Node fetch requires duplex: 'half' for ReadableStream/Readable bodies.
    duplex: 'half'
  }))

  p.strictEqual(targetStreamsSeen, 1, 'streaming body was not replayed')

  await p.completed
})

test('abort stops a source-backed fetch after the first server stream arrives', async (t) => {
  const p = tspl(t, { plan: 3 })

  let streamsSeen = 0
  const controller = new AbortController()

  const { dispatcher, origin } = await createServerAndClient(t, (stream) => {
    streamsSeen++
    // Abort as soon as the server has started processing the request. The
    // source-backed iterator should not be replayed afterwards.
    controller.abort('user abort')
    stream.on('data', () => {})
    stream.on('end', () => {})
  }, undefined, h2AgentFactory)

  const fetchPromise = fetch(`${origin}/`, {
    method: 'POST',
    dispatcher,
    headers: { 'content-type': 'text/plain' },
    body: 'abort-me',
    signal: controller.signal
  })

  await p.rejects(fetchPromise, err => {
    p.strictEqual(err, 'user abort')
    return true
  })
  p.strictEqual(streamsSeen, 1, 'only one server stream started before abort')

  await p.completed
})
