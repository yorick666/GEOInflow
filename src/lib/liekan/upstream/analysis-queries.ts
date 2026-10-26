import { randomUUID } from 'node:crypto';
import type { JobAttempt, PgBoss } from 'pg-boss';
import { hasPermission } from '@/lib/auth';
import { PERMISSIONS, ROLES } from '@/lib/constants';
import type { AnalysisJob } from '../contracts/analysis-acceleration';
import { pageReportParts } from '../contracts/page-report';
import { getPublishedCatalog } from '../crawlers/published';
import { aiExecutor } from './ai-db';
import { withAnalysisPermit } from './analysis-background';
import { analysisPermitTable } from './analysis-permit-context';
import {
  ANALYSIS_QUERY_QUEUE,
  ANALYSIS_RESULT_FORMAT,
  analysisDigest,
  analysisQueryLimits,
  analysisQueryParameters,
  analysisResultFresh,
  decodeAnalysisEnvelope,
  encodeAnalysisEnvelope,
  liveAnalysisTargets,
} from './analysis-query-model';
import { analysisCapacity, analysisTransaction } from './analysis-rollups-db';
import { readAnalyticsVersions } from './analytics-revision';
import {
  analyticsWorkerContext,
  captureBrowserPopulations,
  currentCacheContext,
  noteBrowserPopulation,
} from './cache-context';
import { statisticsCacheTtl } from './cache-policy';
import { authorizeCrawler, type CrawlerActor } from './crawler-access';
import { withPublishedRegistry } from './crawler-registry-context';
import { pauseIdleDashboardRetries } from './dashboard-demand';
import { LiekanError } from './errors';
import { websiteCacheTag } from './query-cache';

type Row = {
  id: string;
  website_id: string;
  actor_id: string;
  target: string;
  parameters: Record<string, unknown>;
  query_key: string;
  strong_version: string;
  catalog_version: string | null;
  epoch: bigint;
  status: AnalysisJob['status'];
  forced: boolean;
  locale: 'zh-CN' | 'en-US';
  result: any;
  result_bytes: number;
  queued_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
  generated_at: Date | null;
  expires_at: Date | null;
  retention_until: Date;
  error_code: string | null;
  lease_token: string | null;
  attempts: number;
  foreground?: boolean;
};
export type AnalysisRegistration = (tx: any, row: Row) => Promise<void>;
const date = (value: Date | null) => (value == null ? null : new Date(value).toISOString());
export function analysisJobDto(row: Row): AnalysisJob {
  return {
    id: row.id,
    websiteId: row.website_id,
    status: row.status,
    queuedAt: date(row.queued_at),
    startedAt: date(row.started_at),
    completedAt: date(row.completed_at),
    generatedAt: row.generated_at ? +new Date(row.generated_at) : null,
    expiresAt: row.expires_at ? +new Date(row.expires_at) : null,
    errorCode: row.error_code,
    retryAfterMs:
      row.attempts === 0 || (row.attempts === 1 && row.status === 'running') ? 1000 : 3000,
    resultUrl: `/liekan/websites/${row.website_id}/analytics-jobs/${row.id}/result`,
  };
}
export async function analysisQueryState(tx: any, websiteId: string, actor: CrawlerActor) {
  const website = await authorizeCrawler(aiExecutor(tx), actor, websiteId);
  const [site] = await tx.$queryRawUnsafe(
    'SELECT s.epoch,s.mode,w.site_number AS "siteNumber" FROM liekan_analysis_site s JOIN website w USING(website_id) WHERE s.website_id=$1::uuid',
    websiteId,
  );
  if (!site) throw new LiekanError('统计加速迁移未完成', 503);
  const { strong } = await readAnalyticsVersions(tx, [websiteCacheTag(websiteId)]);
  const canManage = Boolean(
    website.role === ROLES.admin ||
      website.ownerUserId === actor.id ||
      (website.teamRole && (await hasPermission(website.teamRole, PERMISSIONS.websiteUpdate))),
  );
  return {
    website: { ...website, siteNumber: site.siteNumber, canManage },
    epoch: site.epoch,
    mode: site.mode,
    strong,
  };
}
const state = analysisQueryState;
function projected(row: Row, website: any) {
  const result = decodeAnalysisEnvelope(row.result, row.website_id);
  const value = structuredClone(result.value) as any;
  if (row.target === 'feature:page-report' && value) {
    value.website = {
      id: website.id,
      name: website.name,
      domain: website.domain,
      siteNumber: website.siteNumber,
    };
    value.meta = {
      ...value.meta,
      canManage: website.canManage,
      generatedAt: +new Date(row.generated_at),
      expiresAt: +new Date(row.expires_at),
    };
  }
  for (const population of result.populations) noteBrowserPopulation(row.website_id, population);
  return value;
}
/** Transactional outbox: queued business rows remain discoverable until a worker owns them. */
async function requestAnalysisQueryOnce(
  websiteId: string,
  actor: CrawlerActor,
  target: string,
  input: Record<string, unknown>,
  locale: 'zh-CN' | 'en-US' = 'zh-CN',
  registered?: AnalysisRegistration,
) {
  const parameters = analysisQueryParameters(target, input),
    forced = input.refresh === '1';
  if (Buffer.byteLength(JSON.stringify(parameters)) > 16384)
    throw new LiekanError('统计条件超出上限', 413);
  return analysisTransaction(async tx => {
    const snapshot = await state(tx, websiteId, actor);
    const key = analysisDigest([
      ANALYSIS_RESULT_FORMAT,
      websiteId,
      target,
      parameters,
      locale,
      snapshot.strong,
      String(snapshot.epoch),
    ]);
    const rows = await tx.$queryRawUnsafe<Row[]>(
      'SELECT * FROM liekan_analysis_query WHERE website_id=$1::uuid AND query_key=$2 AND retention_until>clock_timestamp() ORDER BY sequence DESC LIMIT 20',
      websiteId,
      key,
    );
    let existing = rows.find(
      row =>
        ['queued', 'running'].includes(row.status) &&
        (!forced ||
          (row.forced &&
            row.actor_id === actor.id &&
            +new Date(row.queued_at) > Date.now() - 5000)),
    );
    const ready = !forced && !existing && rows.find(row => analysisResultFresh(row));
    if (ready) {
      if (
        (await readAnalyticsVersions(tx, [websiteCacheTag(websiteId)])).strong !== snapshot.strong
      )
        throw new LiekanError('统计条件已变化，请重新读取', 409);
      await registered?.(tx, ready);
      return {
        ready: true as const,
        value: projected(ready, snapshot.website),
        generatedAt: +new Date(ready.generated_at),
        expiresAt: +new Date(ready.expires_at),
      };
    }
    if (existing) {
      const consumer = await tx.$queryRawUnsafe<any[]>(
        'SELECT 1 FROM liekan_analysis_query_consumer WHERE job_id=$1::uuid AND actor_id=$2::uuid',
        existing.id,
        actor.id,
      );
      if (consumer.length) {
        await registered?.(tx, existing);
        const previous = rows.find(row => row.status === 'ready');
        return {
          ready: false as const,
          analysisJob: analysisJobDto(existing),
          ...(previous ? { previousResult: projected(previous, snapshot.website) } : {}),
        };
      }
    }
    const [lock] = await tx.$queryRawUnsafe<{ locked: boolean }[]>(
      'SELECT pg_try_advisory_xact_lock(1277339412) AS locked',
    );
    if (!lock.locked)
      throw new LiekanError('统计任务登记繁忙，请稍后重试', 503, 'analysis-admission-busy');
    // Recheck after the global admission lock: another process may have registered the same input.
    const [concurrent] = await tx.$queryRawUnsafe<Row[]>(
      `SELECT * FROM liekan_analysis_query WHERE website_id=$1::uuid AND query_key=$2 AND status IN('queued','running') AND retention_until>clock_timestamp() AND ($3=false OR (forced AND actor_id=$4::uuid AND queued_at>clock_timestamp()-interval '5 seconds')) ORDER BY sequence DESC LIMIT 1`,
      websiteId,
      key,
      forced,
      actor.id,
    );
    existing = concurrent;
    if (
      forced &&
      rows.some(
        row =>
          row.actor_id === actor.id && row.forced && +new Date(row.queued_at) > Date.now() - 5000,
      ) &&
      !existing
    )
      throw new LiekanError('刷新过于频繁，请稍后重试', 429);
    const capacity = await analysisCapacity(tx, websiteId);
    if (
      BigInt(capacity.globalBytes) >= BigInt(capacity.globalLimit) ||
      BigInt(capacity.siteBytes) >= BigInt(capacity.siteLimit)
    )
      throw new LiekanError('统计结果空间已达上限', 503, 'analysis-capacity-limited');
    const [counts] = await tx.$queryRawUnsafe<{ global: number; site: number; actor: number }[]>(
      `SELECT count(*)::int AS global,count(*) FILTER(WHERE q.website_id=$1::uuid)::int AS site,
       count(*) FILTER(WHERE (q.target=ANY($3::text[]))=$4::boolean AND EXISTS(SELECT 1 FROM liekan_analysis_query_consumer c WHERE c.job_id=q.id AND c.actor_id=$2::uuid))::int AS actor
       FROM liekan_analysis_query q WHERE q.status IN('queued','running') AND q.retention_until>clock_timestamp()`,
      websiteId,
      actor.id,
      liveAnalysisTargets,
      liveAnalysisTargets.includes(target),
    );
    const joined =
      existing &&
      (await tx.$queryRawUnsafe<any[]>(
        'SELECT 1 FROM liekan_analysis_query_consumer WHERE job_id=$1::uuid AND actor_id=$2::uuid',
        existing.id,
        actor.id,
      ));
    if (
      (!existing &&
        (counts.global >= analysisQueryLimits.global || counts.site >= analysisQueryLimits.site)) ||
      (!joined?.length && counts.actor >= analysisQueryLimits.actor)
    )
      throw new LiekanError(
        '后台统计任务已达上限，请等待当前任务完成',
        429,
        'analysis-concurrency-limited',
      );
    let job = existing;
    if (!job) {
      [job] = await tx.$queryRawUnsafe<Row[]>(
        `INSERT INTO liekan_analysis_query(id,website_id,actor_id,target,parameters,query_key,strong_version,catalog_version,epoch,forced,status,locale)
       VALUES($1::uuid,$2::uuid,$3::uuid,$4,$5::jsonb,$6,$7,$8::uuid,$9::bigint,$10,'queued',$11) RETURNING *`,
        randomUUID(),
        websiteId,
        actor.id,
        target,
        JSON.stringify(parameters),
        key,
        snapshot.strong,
        getPublishedCatalog()?.version || null,
        snapshot.epoch,
        forced,
        locale,
      );
    }
    await tx.$executeRawUnsafe(
      'INSERT INTO liekan_analysis_query_consumer(job_id,actor_id) VALUES($1::uuid,$2::uuid) ON CONFLICT DO NOTHING',
      job.id,
      actor.id,
    );
    await registered?.(tx, job);
    const previous = rows.find(row => row.status === 'ready');
    return {
      ready: false as const,
      analysisJob: analysisJobDto(job),
      ...(previous ? { previousResult: projected(previous, snapshot.website) } : {}),
    };
  });
}
const registrations = new Map<string, Promise<unknown>>();
export async function requestAnalysisQuery(
  websiteId: string,
  actor: CrawlerActor,
  target: string,
  input: Record<string, unknown>,
  locale: 'zh-CN' | 'en-US' = 'zh-CN',
  registered?: AnalysisRegistration,
) {
  const key = analysisDigest([
    websiteId,
    actor.id,
    target,
    input,
    locale,
    getPublishedCatalog()?.version,
  ]);
  const pending = registrations.get(key);
  if (pending) await pending.catch(() => {});
  const deadline = Date.now() + 500;
  const run = async () => {
    for (;;) {
      try {
        return await requestAnalysisQueryOnce(websiteId, actor, target, input, locale, registered);
      } catch (error) {
        if (
          !(error instanceof LiekanError) ||
          error.code !== 'analysis-admission-busy' ||
          Date.now() >= deadline
        )
          throw error;
        await new Promise(resolve => setTimeout(resolve, 25));
      }
    }
  };
  const promise = run();
  if (!pending && registrations.size < 128) registrations.set(key, promise);
  try {
    return await promise;
  } finally {
    if (registrations.get(key) === promise) registrations.delete(key);
  }
}
export async function readAnalysisQuery(
  websiteId: string,
  actor: CrawlerActor,
  jobId: string,
  result = false,
) {
  return analysisTransaction(async tx => {
    const snapshot = await state(tx, websiteId, actor);
    const [row] = await tx.$queryRawUnsafe<Row[]>(
      `SELECT q.* FROM liekan_analysis_query q JOIN liekan_analysis_query_consumer c ON c.job_id=q.id AND c.actor_id=$3::uuid
     WHERE q.id=$1::uuid AND q.website_id=$2::uuid`,
      jobId,
      websiteId,
      actor.id,
    );
    if (!row) throw new LiekanError('统计任务不存在或已过期，请重新读取', 410, 'analysis-expired');
    if (row.strong_version !== snapshot.strong || String(row.epoch) !== String(snapshot.epoch))
      throw new LiekanError('统计条件已变化，请重新读取', 410, 'analysis-invalidated');
    if (
      +new Date(row.retention_until) <= Date.now() ||
      ['invalidated', 'expired'].includes(row.status)
    )
      throw new LiekanError('统计任务已过期，请重新读取', 410, 'analysis-expired');
    if (['queued', 'running'].includes(row.status))
      await tx.$executeRawUnsafe(
        'UPDATE liekan_dashboard_snapshot SET last_read_at=clock_timestamp() WHERE job_id=$1::uuid',
        row.id,
      );
    if (result && row.status === 'failed')
      throw new LiekanError(
        '后台统计计算失败，请缩小范围或重新读取',
        503,
        row.error_code || 'analysis-failed',
      );
    if (result && row.status === 'ready' && !analysisResultFresh(row))
      throw new LiekanError('统计结果已过期，请重新读取', 410, 'analysis-expired');
    if (
      result &&
      row.status === 'ready' &&
      (await readAnalyticsVersions(tx, [websiteCacheTag(websiteId)])).strong !== snapshot.strong
    )
      throw new LiekanError('统计条件已变化，请重新读取', 410);
    if (result && row.status === 'ready')
      return {
        ready: true as const,
        value: projected(row, snapshot.website),
        generatedAt: +new Date(row.generated_at),
        expiresAt: +new Date(row.expires_at),
      };
    return { ready: false as const, analysisJob: analysisJobDto(row) };
  });
}
export async function releaseExpiredAnalysisLeases() {
  await analysisTransaction(tx =>
    tx.$executeRawUnsafe(`UPDATE liekan_analysis_query SET status=CASE WHEN attempts>=4 THEN 'failed' ELSE 'queued' END,error_code='analysis-lease-expired',lease_token=NULL,lease_until=NULL
     WHERE status='running' AND lease_until<=clock_timestamp()`),
  );
}
export async function pumpAnalysisQueries(boss: Pick<PgBoss, 'send' | 'update'>) {
  await releaseExpiredAnalysisLeases();
  const rows = await analysisTransaction(async tx => {
    await pauseIdleDashboardRetries(tx);
    await tx.$executeRawUnsafe(
      `DELETE FROM liekan_analysis_query WHERE retention_until<=clock_timestamp()`,
    );
    // Preserve every active request; evict only older completed responses.
    await tx.$executeRawUnsafe(
      `DELETE FROM liekan_analysis_query WHERE id IN(SELECT id FROM(SELECT id,row_number() OVER(PARTITION BY website_id ORDER BY sequence DESC) AS rank FROM liekan_analysis_query WHERE status='ready') r WHERE rank>20)`,
    );
    return tx.$queryRawUnsafe<Row[]>(
      `SELECT q.*,NOT EXISTS(SELECT 1 FROM liekan_dashboard_snapshot d WHERE d.job_id=q.id)
        OR EXISTS(SELECT 1 FROM liekan_dashboard_snapshot d WHERE d.job_id=q.id AND d.last_read_at>clock_timestamp()-interval '2 minutes') AS foreground
       FROM liekan_analysis_query q WHERE status='queued' AND next_attempt_at<=clock_timestamp() ORDER BY queued_at LIMIT 32`,
    );
  });
  for (const row of rows) await handoff(row, boss);
}
async function handoff(row: Row, boss: Pick<PgBoss, 'send' | 'update'>) {
  const priority = row.foreground === false ? 0 : 10;
  // First live deliveries can gain a reader after pickup; pre-arm their bounded contention retries.
  // Delivery retries do not claim the business row or spend a computation attempt.
  const delivery = {
    retryLimit:
      row.foreground !== false || (row.attempts === 0 && liveAnalysisTargets.includes(row.target))
        ? 10
        : 0,
    retryDelay: 1,
    retryBackoff: false,
  };
  const sent = await boss.send(
    ANALYSIS_QUERY_QUEUE,
    { id: row.id },
    {
      singletonKey: row.id,
      priority,
      ...delivery,
      expireInSeconds: 120,
      retentionSeconds: 60,
      deleteAfterSeconds: 3600,
    },
  );
  // Singleton handoffs already in the queue retain their payload and receive current demand priority.
  if (!sent)
    await boss.update(ANALYSIS_QUERY_QUEUE, undefined, {
      singletonKey: row.id,
      priority,
      ...delivery,
    });
}
/** The HTTP boundary can hand off its committed outbox row without waiting for the recovery pump. */
export async function dispatchAnalysisQuery(id: string, boss: Pick<PgBoss, 'send' | 'update'>) {
  const [row] = await analysisTransaction(tx =>
    tx.$queryRawUnsafe<Row[]>(
      `SELECT q.*,NOT EXISTS(SELECT 1 FROM liekan_dashboard_snapshot d WHERE d.job_id=q.id)
     OR EXISTS(SELECT 1 FROM liekan_dashboard_snapshot d WHERE d.job_id=q.id
       AND d.last_read_at>clock_timestamp()-interval '2 minutes') AS foreground
     FROM liekan_analysis_query q WHERE id=$1::uuid AND status='queued' AND next_attempt_at<=clock_timestamp()`,
      id,
    ),
  );
  if (row) await handoff(row, boss);
}
export async function processAnalysisQuery(id: string) {
  const candidate = await analysisTransaction(async tx => {
    await pauseIdleDashboardRetries(tx);
    const [row] = await tx.$queryRawUnsafe<Row[]>(
      `SELECT q.*,NOT EXISTS(SELECT 1 FROM liekan_dashboard_snapshot d WHERE d.job_id=q.id)
       OR EXISTS(SELECT 1 FROM liekan_dashboard_snapshot d WHERE d.job_id=q.id
         AND d.last_read_at>clock_timestamp()-interval '2 minutes') AS foreground
       FROM liekan_analysis_query q WHERE id=$1::uuid AND status='queued' AND next_attempt_at<=clock_timestamp()`,
      id,
    );
    return row;
  });
  if (!candidate) return { acquired: false as const, value: undefined, retryDelivery: false };
  const shortAttempt = candidate.attempts === 0 && liveAnalysisTargets.includes(candidate.target);
  // Unviewed prewarming retains low queue priority and the shared heavy slot.
  const foregroundDemand = candidate.foreground !== false;
  const dashboardLane = shortAttempt && foregroundDemand;
  const result = await withAnalysisPermit(
    dashboardLane ? 'dashboard' : 'interactive',
    async (owned, lease) => {
      const row = await analysisTransaction(async tx => {
        const [row] = await tx.$queryRawUnsafe<Row[]>(
          `UPDATE liekan_analysis_query SET status='running',started_at=clock_timestamp(),lease_token=$2::uuid,
       lease_until=clock_timestamp()+interval '90 seconds',attempts=attempts+1,updated_at=clock_timestamp()
       WHERE id=$1::uuid AND status='queued' AND next_attempt_at<=clock_timestamp()
       AND (attempts=0 AND target=ANY($4::text[]))=$3::boolean RETURNING *`,
          id,
          randomUUID(),
          shortAttempt,
          liveAnalysisTargets,
        );
        return row;
      });
      if (!row) return;
      try {
        return await withPublishedRegistry(async () => {
          const actor = { id: row.actor_id, role: '' };
          const allowed = () =>
            analysisTransaction(async tx => {
              const s = await state(tx, row.website_id, actor);
              return s.strong === row.strong_version && String(s.epoch) === String(row.epoch);
            });
          if (!(await allowed())) throw new LiekanError('统计条件已变化', 410);
          const { dashboardJobClock, publishDashboardSnapshot } = await import(
            './dashboard-snapshots'
          );
          const { withDashboardClock } = await import('./dashboard-clock');
          const cutoff = await analysisTransaction(tx => dashboardJobClock(tx, row.id));
          const started = Date.now();
          const compute = (refresh: boolean) =>
            analyticsWorkerContext(
              row.actor_id,
              { private: true },
              async () => {
                const { executeAnalysisQuery } = await import('./analysis-query-execution');
                const execute = () =>
                  captureBrowserPopulations(() =>
                    executeAnalysisQuery(row.website_id, actor, row.target, row.parameters),
                  );
                const result = await (cutoff === undefined
                  ? execute()
                  : withDashboardClock(cutoff, execute));
                return { ...result, freshness: [...currentCacheContext().results] };
              },
              {
                kind: 'interactive',
                refresh,
                deadline:
                  started + (shortAttempt || row.target === 'native:heatmaps' ? 10000 : 60000),
                reauthorize: allowed,
              },
            );
          let result = await compute(row.forced);
          // A short-lived input cache can expire while a later query finishes. Recompute once within the same deadline.
          if (result.freshness.some(item => item.expiresAt <= Date.now() + 5000))
            result = await compute(true);
          if (result.freshness.some(item => item.expiresAt <= Date.now()))
            throw new LiekanError('统计来源结果已过期，请重新计算', 503, 'analysis-source-expired');
          // Native responses are projected through their existing JSON representation.
          const encoded = encodeAnalysisEnvelope(result.value, result.populations, row.website_id);
          if (!owned()) throw new LiekanError('后台计算租约失效', 410);
          await analysisTransaction(async tx => {
            const [permit] = await tx.$queryRawUnsafe(
              `SELECT id FROM ${analysisPermitTable(lease)} WHERE id AND token=$1::uuid AND epoch=$2::bigint AND lease_until>clock_timestamp() FOR SHARE`,
              lease.token,
              lease.epoch,
            );
            if (!permit) throw new LiekanError('后台计算租约失效', 410);
            const snapshot = await state(tx, row.website_id, actor);
            if (
              snapshot.strong !== row.strong_version ||
              String(snapshot.epoch) !== String(row.epoch)
            )
              throw new LiekanError('统计条件已变化', 410);
            const endAt = Number(
              row.parameters.endAt ?? +new Date(row.parameters.endDate as string),
            );
            // Page-report may include today's dedicated counters even for a historical selection.
            const ttl =
              row.target === 'feature:page-report' &&
              pageReportParts[String(row.parameters.resource)]?.today
                ? 30000
                : statisticsCacheTtl(endAt, String(row.parameters.timezone || 'UTC'));
            const generatedAt = result.freshness.length
              ? Math.min(...result.freshness.map(item => item.generatedAt))
              : started;
            const expiresAt = result.freshness.length
              ? Math.min(...result.freshness.map(item => item.expiresAt))
              : Date.now() + ttl;
            if (expiresAt <= Date.now())
              throw new LiekanError(
                '统计来源结果已过期，请重新计算',
                503,
                'analysis-source-expired',
              );
            // Capacity can change while a task runs. Charge the prospective result before publishing it.
            const capacity = await analysisCapacity(tx, row.website_id);
            if (
              BigInt(capacity.globalBytes) + BigInt(Buffer.byteLength(encoded) * 2) >=
                BigInt(capacity.globalLimit) ||
              BigInt(capacity.siteBytes) + BigInt(Buffer.byteLength(encoded) * 2) >=
                BigInt(capacity.siteLimit)
            )
              throw new LiekanError('统计结果空间已达上限', 413, 'analysis-capacity-limited');
            const published = await tx.$executeRawUnsafe(
              `UPDATE liekan_analysis_query SET status='ready',result=$3::jsonb,result_bytes=$4,generated_at=$5::timestamptz,
           completed_at=clock_timestamp(),expires_at=$6::timestamptz,retention_until=clock_timestamp()+interval '24 hours',
           lease_token=NULL,lease_until=NULL,error_code=NULL,updated_at=clock_timestamp()
           WHERE id=$1::uuid AND lease_token=$2::uuid AND status='running' AND lease_until>clock_timestamp() AND $6::timestamptz>clock_timestamp()`,
              row.id,
              row.lease_token,
              encoded,
              Buffer.byteLength(encoded),
              new Date(generatedAt),
              new Date(expiresAt),
            );
            if (published !== 1) {
              if (expiresAt <= Date.now())
                throw new LiekanError(
                  '统计来源结果已过期，请重新计算',
                  503,
                  'analysis-source-expired',
                );
              throw new LiekanError('后台计算租约失效', 410);
            }
            await publishDashboardSnapshot(tx, {
              id: row.id,
              result: JSON.parse(encoded),
              result_bytes: Buffer.byteLength(encoded),
              generated_at: new Date(generatedAt),
            });
            await tx.$executeRawUnsafe(
              `DELETE FROM liekan_analysis_query WHERE website_id=$1::uuid AND id IN(
            SELECT id FROM liekan_analysis_query WHERE website_id=$1::uuid AND status='ready' ORDER BY sequence DESC OFFSET 20)`,
              row.website_id,
            );
          });
        }, row.catalog_version || undefined);
      } catch (error) {
        const status = error instanceof LiekanError ? error.status : 500;
        const code = error instanceof LiekanError ? error.code : 'analysis-compute-failed';
        await analysisTransaction(async tx => {
          await tx.$executeRawUnsafe(
            `UPDATE liekan_analysis_query SET status=CASE WHEN $3=410 OR $3 IN(401,403,404) THEN 'invalidated'
       WHEN attempts>=4 OR $3 IN(400,413) THEN 'failed' ELSE 'queued' END,error_code=$4,
       next_attempt_at=clock_timestamp()+(CASE attempts WHEN 1 THEN 5 WHEN 2 THEN 30 ELSE 120 END)*interval '1 second',
       lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1::uuid AND lease_token=$2::uuid`,
            row.id,
            row.lease_token,
            status,
            code,
          );
          await pauseIdleDashboardRetries(tx);
        });
        return error instanceof Error ? error : new Error('analysis-compute-failed');
      }
    },
  );
  return { ...result, retryDelivery: foregroundDemand && !result.acquired };
}

/** A contended delivery yields its queue priority while durable computation retries keep their limits. */
export async function processAnalysisQueryDelivery(
  job: JobAttempt & { data: { id: string } },
  boss: Pick<PgBoss, 'fail' | 'update'>,
) {
  const result = await processAnalysisQuery(job.data.id);
  if ('retryDelivery' in result && result.retryDelivery) {
    const failed = await boss.fail(ANALYSIS_QUERY_QUEUE, job, new Error('analysis-permit-busy'));
    if ('affected' in failed && failed.affected === 1)
      await boss.update(ANALYSIS_QUERY_QUEUE, undefined, { id: job.id, priority: 0 });
  }
}
