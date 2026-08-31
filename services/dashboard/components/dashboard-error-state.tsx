import Link from "next/link";

export function DashboardErrorState({ message }: { message: string }) {
  return (
    <main className="error-shell">
      <section className="error-card" aria-labelledby="dashboard-error-title">
        <div className="brand-mark" aria-hidden="true">
          RF
        </div>
        <p className="eyebrow">RiskFlow control room</p>
        <h1 id="dashboard-error-title">Operational view unavailable</h1>
        <p>{message}</p>
        <p className="error-guidance">
          Check the payment API and server-side dashboard configuration, then
          refresh.
        </p>
        <Link className="button" href="/">
          Retry dashboard
        </Link>
      </section>
    </main>
  );
}
