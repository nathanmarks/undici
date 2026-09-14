'use strict'

const { kBodyReplayable } = require('../../core/symbols')

const kStream = Symbol('stream')
const kReplay = Symbol('replay')
const kCallbacks = Symbol('callbacks')
function noop () {}

class ReplayableBodyIterator {
  constructor (stream, callbacks) {
    this.callbacks = callbacks
    this.reader = stream?.getReader() ?? null
    this.closed = stream == null
    this.reading = false
    this.pendingChunkLength = null
  }

  [Symbol.asyncIterator] () {
    return this
  }

  async next () {
    if (this.closed) {
      return { done: true, value: undefined }
    }

    try {
      if (this.pendingChunkLength != null) {
        const length = this.pendingChunkLength
        this.pendingChunkLength = null
        this.callbacks.onChunk?.(length)
      }

      if (this.callbacks.isCancelled()) {
        this.cancel()
        return { done: true, value: undefined }
      }

      this.reading = true
      const { done, value } = await this.reader.read()
      if (this.closed) {
        return { done: true, value: undefined }
      }

      if (done) {
        if (!this.callbacks.isCancelled()) {
          this.callbacks.onEnd?.()
        }
        this.closed = true
        return { done: true, value: undefined }
      }

      this.pendingChunkLength = value.byteLength
      return { done: false, value }
    } catch (err) {
      const reportError = !this.closed && !this.callbacks.isCancelled()
      this.cancel(err)
      if (reportError) {
        this.callbacks.onError?.(err)
      }
      return { done: true, value: undefined }
    } finally {
      this.reading = false
      if (this.closed) {
        this.release()
      }
    }
  }

  async return (reason) {
    this.cancel(reason)
    return { done: true, value: undefined }
  }

  cancel (reason) {
    if (this.reader == null) {
      return
    }

    this.closed = true
    try {
      this.reader.cancel(reason).catch(noop)
    } catch {}

    if (!this.reading) {
      this.release()
    }
  }

  release () {
    if (this.reader == null) {
      return
    }

    try {
      this.reader.releaseLock()
    } catch {}
    this.reader = null
  }
}

/**
 * A request body that can be iterated more than once because Fetch retained a
 * factory over the bytes captured during body extraction. HTTP/2 GOAWAY and
 * REFUSED_STREAM retries can therefore replay string, URLSearchParams,
 * BufferSource, Blob, and FormData bodies while still reporting request-body
 * progress and end-of-body callbacks per attempt.
 *
 * The first iteration consumes the stream that Fetch already created. Only a
 * replay invokes the retained factory, avoiding another stream allocation on
 * the common path and ensuring mutable inputs are not read again.
 *
 * Source-null streaming bodies have no replay factory and remain single-use.
 *
 * @internal
 */
class ReplayableBody {
  constructor (stream, length, replay, callbacks) {
    this.length = length
    this[kStream] = stream
    this[kReplay] = replay
    this[kCallbacks] = callbacks
    this[kBodyReplayable] = true
  }

  [Symbol.asyncIterator] () {
    const callbacks = this[kCallbacks]
    const stream = callbacks.isCancelled()
      ? null
      : this[kStream] ?? this[kReplay]()
    this[kStream] = null

    return new ReplayableBodyIterator(stream, callbacks)
  }
}

module.exports = { ReplayableBody }
