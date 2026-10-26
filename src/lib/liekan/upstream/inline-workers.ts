import { after } from 'next/server';

const INLINE_WORKER_ID = 'vercel-inline';

export function continueOnVercel(work: () => Promise<unknown>) {
  if (process.env.VERCEL !== '1') return;
  try {
    after(() =>
      work().catch(() => {
        console.error('Liekan inline worker failed');
      }),
    );
  } catch {
    /* Tests and scripts have no request scope. */
  }
}

async function noteInlineHeartbeat() {
  const { query } = await import('./server');
  await query(
    `INSERT INTO liekan_worker_state(id,heartbeat_at,version) VALUES($1,clock_timestamp(),'vercel-inline') ON CONFLICT(id) DO UPDATE SET heartbeat_at=clock_timestamp(),version=EXCLUDED.version`,
    INLINE_WORKER_ID,
  );
}

export function continueAnalysisQuery(id: string) {
  continueOnVercel(async () => {
    await noteInlineHeartbeat().catch(() => {});
    const { processAnalysisQuery, releaseExpiredAnalysisLeases } = await import(
      './analysis-queries'
    );
    await releaseExpiredAnalysisLeases();
    await processAnalysisQuery(id);
  });
}

export function continueGeoSnapshot() {
  continueOnVercel(async () => {
    await noteInlineHeartbeat().catch(() => {});
    const { processGeoSnapshotJob, releaseExpiredGeoLeases } = await import(
      './geo-weight-snapshots'
    );
    const { getQueue } = await import('../log-ingestion/queue');
    await releaseExpiredGeoLeases();
    await processGeoSnapshotJob({ headId: 'inline' }, await getQueue());
  });
}

export function continueLogBatch(batchId: string) {
  continueOnVercel(async () => {
    await noteInlineHeartbeat().catch(() => {});
    const { processBatch } = await import('../log-ingestion/service');
    await processBatch(batchId);
  });
}

export async function drainInlineWork(limits = { analysis: 8, geo: 4, logs: 8 }) {
  const { processAnalysisQuery, releaseExpiredAnalysisLeases } = await import('./analysis-queries');
  const { analysisTransaction } = await import('./analysis-rollups-db');
  await releaseExpiredAnalysisLeases();
  const rows = await analysisTransaction(tx =>
    tx.$queryRawUnsafe<{ id: string }[]>(
      `SELECT id FROM liekan_analysis_query WHERE status='queued' AND next_attempt_at<=clock_timestamp() ORDER BY queued_at LIMIT $1`,
      limits.analysis,
    ),
  );
  let analysis = 0;
  for (const row of rows) {
    const result = await processAnalysisQuery(row.id);
    if (result.acquired) analysis += 1;
  }
  let geo = 0;
  try {
    const { processGeoSnapshotJob, releaseExpiredGeoLeases } = await import(
      './geo-weight-snapshots'
    );
    const { getQueue } = await import('../log-ingestion/queue');
    await releaseExpiredGeoLeases();
    const boss = await getQueue();
    for (let step = 0; step < limits.geo; step += 1) {
      await processGeoSnapshotJob({ headId: 'cron' }, boss);
      geo += 1;
    }
  } catch {
    console.error('Liekan inline geo drain failed');
  }
  let logs = 0;
  try {
    const { query } = await import('./server');
    const { processBatch } = await import('../log-ingestion/service');
    const batches = await query<{ id: string }>(
      `SELECT id FROM liekan_ingest_batch WHERE state='pending' ORDER BY received_at LIMIT $1`,
      limits.logs,
    );
    for (const batch of batches) {
      await processBatch(batch.id);
      logs += 1;
    }
  } catch {
    console.error('Liekan inline log drain failed');
  }
  await noteInlineHeartbeat().catch(() => {});
  return { analysis, geo, logs };
}
