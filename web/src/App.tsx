import { useCallback, useEffect, useMemo, useState } from "react";
import "./App.css";

type ResourceConfig = {
  name: string;
  slots: string[];
  capacity: number;
};

type Scenario = {
  id: string;
  label: string;
  description: string;
  resources: ResourceConfig[];
};

type SlotStatus = {
  slot: string;
  held_by: { workflow_id: string; run_id: string; started_at: string | null } | null;
};

type ResourceStatus = {
  resource: string;
  capacity: number;
  slots: SlotStatus[];
  held_count: number;
  free_count: number;
};

type RecentItem = {
  id: string;
  run_id: string;
  status: string;
  start_time: string | null;
  close_time: string | null;
};

type ConfigInfo = {
  capacities: Record<string, number>;
  min: number;
  max: number;
};

const POLL_MS = 1000;

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, { headers: { "content-type": "application/json" }, ...init });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`${r.status} ${r.statusText}: ${text}`);
  }
  return r.json() as Promise<T>;
}

function ResourcePanel({
  resource,
  capacity,
  capRange,
  onCapacityChange,
}: {
  resource: ResourceConfig;
  capacity: number;
  capRange: { min: number; max: number };
  onCapacityChange: (next: number) => void;
}) {
  const [status, setStatus] = useState<ResourceStatus | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function tick() {
      try {
        const s = await api<ResourceStatus>(`/api/status/${encodeURIComponent(resource.name)}`);
        if (!cancelled) setStatus(s);
      } catch {
        // backend not ready -- swallow and retry
      }
    }
    void tick();
    const t = setInterval(tick, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, [resource.name, capacity]);

  const slots = status?.slots ?? [];
  const held = status?.held_count ?? 0;
  const cap = status?.capacity ?? capacity;
  const utilization = cap === 0 ? 0 : Math.round((held / cap) * 100);

  return (
    <div className="resource-panel">
      <div className="resource-header">
        <span className="resource-name">{resource.name}</span>
        <span className="resource-count">
          {held} / {cap} held
          <span className="resource-pct"> · {utilization}%</span>
        </span>
      </div>

      <div className="capacity-row">
        <label className="capacity-label">
          slots
          <input
            type="range"
            min={capRange.min}
            max={capRange.max}
            step={1}
            value={capacity}
            onChange={(e) => onCapacityChange(parseInt(e.target.value, 10))}
          />
          <span className="count-value">{capacity}</span>
        </label>
      </div>

      <div className="slot-grid">
        {slots.map((s) => (
          <div
            key={s.slot}
            className={`slot ${s.held_by ? "held" : "free"}`}
            title={
              s.held_by
                ? `${s.slot}\nheld by ${s.held_by.workflow_id}`
                : `${s.slot} (free)`
            }
          >
            <span className="slot-name">{s.slot}</span>
            {s.held_by && (
              <span className="slot-holder">{s.held_by.workflow_id}</span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function RecentRuns({ scenario }: { scenario: string }) {
  const [items, setItems] = useState<RecentItem[]>([]);

  useEffect(() => {
    let cancelled = false;
    async function tick() {
      try {
        const r = await api<{ items: RecentItem[] }>(
          `/api/recent/${scenario}?limit=15`,
        );
        if (!cancelled) setItems(r.items);
      } catch {
        // ignore
      }
    }
    void tick();
    const t = setInterval(tick, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, [scenario]);

  if (items.length === 0) {
    return <div className="recent-empty">no recent runs yet</div>;
  }

  return (
    <ul className="recent-list">
      {items.map((it) => (
        <li key={it.run_id} className={`recent-item status-${it.status.toLowerCase()}`}>
          <span className="recent-id">{it.id}</span>
          <span className="recent-status">{it.status}</span>
        </li>
      ))}
    </ul>
  );
}

function ScenarioCard({
  scenario,
  capacities,
  capRange,
  setCapacity,
}: {
  scenario: Scenario;
  capacities: Record<string, number>;
  capRange: { min: number; max: number };
  setCapacity: (resource: string, value: number) => void;
}) {
  const [count, setCount] = useState(10);
  const [running, setRunning] = useState(false);
  const [lastResult, setLastResult] = useState<string | null>(null);

  async function runBurst() {
    setRunning(true);
    setLastResult(null);
    try {
      const r = await api<{ scenario: string; count: number; ids: string[] }>(
        `/api/run/${scenario.id}?count=${count}`,
        { method: "POST" },
      );
      setLastResult(`launched ${r.count} workflows`);
    } catch (e) {
      setLastResult(`error: ${(e as Error).message}`);
    } finally {
      setRunning(false);
    }
  }

  return (
    <section className="card">
      <header className="card-header">
        <h2>{scenario.label}</h2>
        <p>{scenario.description}</p>
      </header>

      <div className="card-controls">
        <label>
          burst size
          <input
            type="range"
            min={1}
            max={50}
            step={1}
            value={count}
            onChange={(e) => setCount(parseInt(e.target.value, 10))}
          />
          <span className="count-value">{count}</span>
        </label>
        <button onClick={runBurst} disabled={running}>
          {running ? "Launching..." : `Run ${count}`}
        </button>
      </div>

      {lastResult && <div className="card-result">{lastResult}</div>}

      <div className="card-resources">
        {scenario.resources.map((r) => {
          const liveCapacity = capacities[r.name] ?? r.capacity;
          const liveResource: ResourceConfig = {
            ...r,
            capacity: liveCapacity,
          };
          return (
            <ResourcePanel
              key={r.name}
              resource={liveResource}
              capacity={liveCapacity}
              capRange={capRange}
              onCapacityChange={(next) => setCapacity(r.name, next)}
            />
          );
        })}
      </div>

      <div className="card-recent">
        <h3>Recent runs</h3>
        <RecentRuns scenario={scenario.id} />
      </div>
    </section>
  );
}

export default function App() {
  const [scenarios, setScenarios] = useState<Scenario[]>([]);
  const [config, setConfig] = useState<ConfigInfo | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const [s, c] = await Promise.all([
          api<{ scenarios: Scenario[] }>("/api/scenarios"),
          api<ConfigInfo>("/api/config"),
        ]);
        setScenarios(s.scenarios);
        setConfig(c);
      } catch (e) {
        setError(`could not reach API: ${(e as Error).message}`);
      }
    }
    void load();
  }, []);

  const setCapacity = useCallback(async (resource: string, value: number) => {
    setConfig((prev) =>
      prev
        ? { ...prev, capacities: { ...prev.capacities, [resource]: value } }
        : prev,
    );
    try {
      await api(`/api/config/${encodeURIComponent(resource)}?capacity=${value}`, {
        method: "POST",
      });
    } catch {
      // ignore -- next poll will resync
    }
  }, []);

  const capRange = useMemo(
    () => ({ min: config?.min ?? 1, max: config?.max ?? 16 }),
    [config],
  );

  const capacities = config?.capacities ?? {};

  const grid = useMemo(
    () =>
      scenarios.map((s) => (
        <ScenarioCard
          key={s.id}
          scenario={s}
          capacities={capacities}
          capRange={capRange}
          setCapacity={setCapacity}
        />
      )),
    [scenarios, capacities, capRange, setCapacity],
  );

  return (
    <main>
      <header className="page-header">
        <div className="page-title">
          <img
            src="/temporal-symbol-light.png"
            alt=""
            className="temporal-logo"
            width={40}
            height={40}
            decoding="async"
          />
          <h1>Temporal Workflow Throttler</h1>
        </div>
        <p className="page-tagline">
          Click <em>Run</em> on any card below and watch the slot grid fill
          while extra workflows queue up behind it. Use the{" "}
          <strong>slots</strong> slider to change capacity for the next burst.
        </p>
        <details className="page-explainer">
          <summary>How it works</summary>
          <p>
            A <strong>pool</strong> represents a fixed set of named resource
            slots (for example four GPUs or three database connections). Any
            workflow that wants to do the protected work must first{" "}
            <strong>take a slot</strong>. When all slots are taken, additional
            workflows back off and retry. When the work finishes the workflow{" "}
            <strong>gives the slot back</strong>, and whichever waiter probes
            the freed slot first wins it (the order is{" "}
            <strong>not FIFO</strong>; waiters race for the next opening).
            This demo shows how Temporal can gate or throttle work at the
            workflow level and manage hard resource constraints, all without
            adding any external infrastructure.
          </p>
          <p>
            Each slot is itself a small <strong>child workflow</strong>, and
            its workflow id (for example{" "}
            <code>permit:gpu-pool:gpu-2</code>) is the lock. Temporal does not
            allow two running workflows with the same id, so an attempt to
            start a second child for an already held slot fails atomically
            with <code>WorkflowAlreadyStartedError</code> and the caller
            simply tries the next slot. To release, the holder signals the
            slot workflow, which completes and frees its id for reuse. The
            lock state lives entirely in Temporal, so it survives worker
            restarts, crashes and deploys. If a holder dies without
            releasing, the slot workflow's lease timer fires and the slot
            opens up again on its own.
          </p>
        </details>
      </header>

      {error && <div className="banner error">{error}</div>}
      {scenarios.length === 0 && !error && (
        <div className="banner">Loading scenarios...</div>
      )}

      <div className="grid">{grid}</div>

      <footer className="page-footer">
        <span>
          API:&nbsp;
          <code>/api</code> &nbsp;·&nbsp; polling {POLL_MS}ms &nbsp;·&nbsp;
          burst max 50 &nbsp;·&nbsp; slots {capRange.min}&ndash;{capRange.max}
        </span>
      </footer>
    </main>
  );
}
