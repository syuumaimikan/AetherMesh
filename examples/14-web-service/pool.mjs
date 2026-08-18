/**
 * A pool of mesh connections.
 *
 * One connection is one queue. The client protocol matches replies to requests
 * in order, so a request waiting on a slow task holds up every request behind
 * it on that socket — head-of-line blocking, and it is easy to mistake for the
 * mesh being slow when it is the socket being busy.
 *
 * A web server handling concurrent requests wants several connections. This is
 * the smallest pool that does that honestly: a fixed set, handed out one at a
 * time, with callers queueing when all of them are busy.
 */

import { AetherMesh } from "../../sdk/typescript/src/index.ts";

export class MeshPool {
  #free = [];
  #waiting = [];
  #all = [];

  /** Opens `size` connections. */
  static async connect(options = {}, size = 8) {
    const pool = new MeshPool();
    for (let index = 0; index < size; index += 1) {
      const mesh = await AetherMesh.connect(options);
      pool.#all.push(mesh);
      pool.#free.push(mesh);
    }
    return pool;
  }

  get size() {
    return this.#all.length;
  }

  /** Connections not currently handling a request. */
  get idle() {
    return this.#free.length;
  }

  /**
   * Runs `work` with a connection to itself, and gives it back afterwards.
   *
   * Borrowing rather than handing out a connection: a caller that forgets to
   * return one shrinks the pool for the rest of the process, and that failure
   * shows up an hour later as "the site got slow".
   */
  async use(work) {
    const mesh = await this.#acquire();
    try {
      return await work(mesh);
    } finally {
      this.#release(mesh);
    }
  }

  async #acquire() {
    const free = this.#free.pop();
    if (free) return free;
    return new Promise((resolve) => this.#waiting.push(resolve));
  }

  #release(mesh) {
    const next = this.#waiting.shift();
    if (next) {
      next(mesh);
      return;
    }
    this.#free.push(mesh);
  }

  close() {
    for (const mesh of this.#all) mesh.close();
    this.#all = [];
    this.#free = [];
  }
}
