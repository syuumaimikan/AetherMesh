/**
 * Fires concurrent requests at the service and reports what they cost.
 *
 *   node load.mjs [concurrency] [iterations]
 *
 * Run it against a server started with POOL_SIZE=1 and again with the default
 * pool: the mesh is the same, the difference is how many queues the service
 * has to hand the work to.
 */

const CONCURRENCY = Number(process.argv[2] ?? 16);
const ITERATIONS = process.argv[3] ?? "20000000";
const BASE = process.env.BASE ?? "http://127.0.0.1:8081";

async function one() {
  const started = performance.now();
  const response = await fetch(`${BASE}/api/work?iterations=${ITERATIONS}`, {
    method: "POST",
  });
  const body = await response.json();
  return { wall: performance.now() - started, taskMs: body.taskMs, node: body.nodeId };
}

const started = performance.now();
const results = await Promise.all(Array.from({ length: CONCURRENCY }, one));
const wall = performance.now() - started;

const waits = results.map((r) => r.wall).sort((a, b) => a - b);
const work = results.reduce((sum, r) => sum + (r.taskMs ?? 0), 0);
const nodes = new Set(results.map((r) => r.node)).size;

console.log(`${CONCURRENCY} concurrent requests, ${ITERATIONS} iterations each`);
console.log(`  wall            ${wall.toFixed(0)} ms`);
console.log(`  median request  ${waits[Math.floor(waits.length / 2)].toFixed(0)} ms`);
console.log(`  slowest request ${waits.at(-1).toFixed(0)} ms`);
console.log(`  work done       ${work.toFixed(0)} ms across ${nodes} node(s)`);
console.log(`  parallelism     ${(work / wall).toFixed(1)}x`);
