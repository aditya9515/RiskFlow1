import { DashboardErrorState } from "@/components/dashboard-error-state";
import { DashboardView } from "@/components/dashboard-view";
import {
  DashboardLoadError,
  type DashboardSnapshot,
  loadDashboardSnapshot,
} from "@/lib/dashboard";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function Home() {
  const result = await loadSnapshotResult();
  if (!result.ok) {
    return <DashboardErrorState message={result.error} />;
  }
  return <DashboardView snapshot={result.snapshot} />;
}

type SnapshotResult =
  { ok: true; snapshot: DashboardSnapshot } | { ok: false; error: string };

async function loadSnapshotResult(): Promise<SnapshotResult> {
  try {
    const snapshot = await loadDashboardSnapshot();
    return { ok: true, snapshot };
  } catch (error) {
    const message =
      error instanceof DashboardLoadError
        ? error.userMessage
        : "The dashboard could not load its operational snapshot.";
    console.error(
      "dashboard snapshot load failed",
      error instanceof Error ? error.message : "unknown error",
    );
    return { ok: false, error: message };
  }
}
