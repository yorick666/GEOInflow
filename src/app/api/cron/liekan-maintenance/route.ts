import { drainInlineWork } from '@/lib/liekan/upstream/inline-workers';

export const maxDuration = 300;

export async function GET(request: Request) {
  const secret = process.env.CRON_SECRET;
  if (!secret || request.headers.get('authorization') !== `Bearer ${secret}`) {
    return Response.json({ error: 'Unauthorized' }, { status: 401 });
  }
  try {
    const { bootstrapAnalysisHistory, reconcileAnalysisRollups } = await import(
      '@/lib/liekan/upstream/analysis-rollups-worker'
    );
    const { pumpDashboardDemand } = await import('@/lib/liekan/upstream/dashboard-snapshots');
    await bootstrapAnalysisHistory().catch(() => {});
    await reconcileAnalysisRollups().catch(() => {});
    await pumpDashboardDemand().catch(() => {});
  } catch {
    console.error('Liekan maintenance prelude failed');
  }
  const drained = await drainInlineWork();
  return Response.json({ ok: true, ...drained });
}
