import QuickLRU from 'quick-lru';
import { finalize, map, share, take, tap } from 'rxjs/operators';
import { from, fromEvent, lastValueFrom, merge, of, zip } from 'rxjs';
import AbortController from 'node-abort-controller'; // should be noop on browser

import { BaseSource } from './basesource.js';
import { AbortError } from '../utils.js';

const range = (s, e) => new Array(e - s).fill(0).map((x, i) => s + i);

function blockedSource(blockSize, cacheSize, source) {
  const cache = new QuickLRU({ maxSize: cacheSize }); // block cache
  const queue = new Map();                            // outstanding block requests

  const toCache = id => block => cache.set(id, block);
  const responseData = resp => resp[0].data;
  const sliceForBlock = id => ({ offset: id * blockSize, length: blockSize });
  const cleanUp = (ac, id) => () => {
    ac.abort(); // If unsubed, we need to abort. On complete or error, this is a noop.
    queue.delete(id)
  };

  // Dispatch fetch of one block. Add block to cache when complete. Save the
  // fetch in queue, reference count it, and abort it if the count goes to
  // zero (i.e. all requests for this block are cancelled).
  const fetchBlock = id => {
    const ac = new AbortController();
    const request = from(source.fetch([sliceForBlock(id)], ac.signal))
        .pipe(map(responseData), tap(toCache(id)), finalize(cleanUp(ac, id)), share());
    queue.set(id, request);
    return request;
  }

  const cachedOrQueued = block =>
    cache.has(block) ? of(cache.get(block)) :
    queue.has(block) ? queue.get(block) :
    fetchBlock(block);

  // compute block indices for a slice
  const toBlocks = ({offset, length}) =>
    range(~~(offset / blockSize), ~~((offset + length - 1) / blockSize) + 1);

  // concatenate a list of buffers
  const concat = blocks => {
    const result = new Uint8Array(blocks.length * blockSize);
    blocks.forEach((b, i) => result.set(new Uint8Array(b), i * blockSize));
    return result;
  };

  // build slice from contiguous blocks
  const copySlice = slice => blocks => {
    const start = slice.offset % blockSize;
    return concat(blocks).slice(start, start + slice.length).buffer;
  }

  const errObs = signal => fromEvent(signal, 'abort')
    .pipe(map(() => { throw new AbortError(); }));

  // If signal was passed in, listen for abort & throw AbortError.
  // take(1) is so errObs won't hold the merge open after fetchObs is complete.
  const setSignal = (signal, fetchObs) =>
     signal ? merge(fetchObs, errObs(signal)).pipe(take(1)) : fetchObs;

  const fetchSlice = slice =>
    zip(toBlocks(slice).map(cachedOrQueued)).pipe(map(copySlice(slice)));

  return (slices, signal) => {
    const result = zip(slices.map(fetchSlice));
    // handle abort signal, and return as a promise
    return lastValueFrom(setSignal(signal, result));
  }
}

export class BlockedSource extends BaseSource {
  /**
   *
   * @param {BaseSource} source The underlying source that shall be blocked and cached
   * @param {object} options
   * @param {number} [options.blockSize]
   * @param {number} [options.cacheSize]
   */
  constructor(source, { blockSize = 65536, cacheSize = 100 } = {}) {
    super();
    this.source = source;
    this.fetch = blockedSource(blockSize, cacheSize, source);
  }

  get fileSize() {
    return this.source.fileSize;
  }
}
