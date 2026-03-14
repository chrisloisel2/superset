import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { fetchSession, fetchTracker, fetchPince, fetchSessionMetadata, fetchSessionsMetadata } from "../api/client";
import TrackerTable from "../components/data/TrackerTable";
import PinceTable   from "../components/data/PinceTable";
import DataChart    from "../components/data/DataChart";

const TABS = ["Tracker", "Pince 1", "Pince 2"];

function StatCard({ label, value, unit }) {
  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4 text-center shadow-sm">
      <p className="text-xs text-gray-400 uppercase tracking-wide">{label}</p>
      <p className="text-xl font-bold text-gray-700 mt-1">
        {value ?? "—"}
        {unit && <span className="text-sm font-normal text-gray-400 ml-1">{unit}</span>}
      </p>
    </div>
  );
}

function summarizeMetadataValue(value) {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return `Array(${value.length})`;
  if (typeof value === "object") return `Objet (${Object.keys(value).length} champs)`;
  if (typeof value === "boolean") return value ? "true" : "false";
  const s = String(value);
  return s.length > 80 ? `${s.slice(0, 80)}...` : s;
}

export default function SessionDetailPage() {
  const { id }                    = useParams();
  const [summary, setSummary]     = useState(null);
  const [activeTab, setActiveTab] = useState("Tracker");
  const [tableData, setTableData] = useState([]);
  const [loading, setLoading]     = useState(false);
  const [error, setError]         = useState(null);
  const [metadata, setMetadata]   = useState(null);
  const [metadataError, setMetadataError] = useState(null);
  const [metaQuery, setMetaQuery] = useState("");
  const [metaScope, setMetaScope] = useState("current");
  const [allSessionsMeta, setAllSessionsMeta] = useState([]);
  const [globalMatches, setGlobalMatches] = useState([]);
  const [selectedMetaKeys, setSelectedMetaKeys] = useState({});

  // Load session summary once
  useEffect(() => {
    fetchSession(id)
      .then(r => setSummary(r.data))
      .catch(e => setError(e.response?.data?.error || e.message));
  }, [id]);

  useEffect(() => {
    setMetadata(null);
    setMetadataError(null);
    fetchSessionMetadata(id)
      .then((r) => setMetadata(r.data.metadata || null))
      .catch((e) => setMetadataError(e.response?.data?.error || e.message));
  }, [id]);

  useEffect(() => {
    if (!metadata || typeof metadata !== "object") {
      setSelectedMetaKeys({});
      return;
    }
    const next = {};
    Object.keys(metadata).forEach((k) => {
      next[k] = true;
    });
    setSelectedMetaKeys(next);
  }, [metadata]);

  // Load tab data
  useEffect(() => {
    setLoading(true);
    setTableData([]);
    const loaders = {
      "Tracker": () => fetchTracker(id, { limit: 100 }),
      "Pince 1": () => fetchPince(id, 1, { limit: 100 }),
      "Pince 2": () => fetchPince(id, 2, { limit: 100 }),
    };
    loaders[activeTab]()
      .then(r => setTableData(r.data.data ?? []))
      .catch(e => setError(e.response?.data?.error || e.message))
      .finally(() => setLoading(false));
  }, [id, activeTab]);

  useEffect(() => {
    if (metaScope !== "all") return;
    if (allSessionsMeta.length > 0) return;
    fetchSessionsMetadata()
      .then((r) => setAllSessionsMeta(r.data.sessions || []))
      .catch((e) => setMetadataError(e.response?.data?.error || e.message));
  }, [metaScope, allSessionsMeta.length]);

  useEffect(() => {
    const q = metaQuery.trim().toLowerCase();
    if (metaScope !== "all" || !q) {
      setGlobalMatches([]);
      return;
    }

    const matches = (allSessionsMeta || []).filter((row) => {
      const sessionDir = row.session_dir || "";
      const m = row.metadata || {};
      return `${sessionDir} ${JSON.stringify(m)}`.toLowerCase().includes(q);
    });
    setGlobalMatches(matches.slice(0, 20));
  }, [metaScope, metaQuery, allSessionsMeta]);

  const ts = summary?.tracker_stats ?? {};

  // Compute duration
  let duration = "—";
  if (ts.start_ts && ts.end_ts) {
    const diff = (new Date(ts.end_ts) - new Date(ts.start_ts)) / 1000;
    duration = `${diff.toFixed(1)} s`;
  }

  const metadataKeys = metadata && typeof metadata === "object" ? Object.keys(metadata) : [];
  const filteredMetadataKeys = metadataKeys.filter((key) => {
    const q = metaQuery.trim().toLowerCase();
    if (!q) return true;
    const valueStr = JSON.stringify(metadata[key] ?? "");
    return `${key} ${valueStr}`.toLowerCase().includes(q);
  });

  const selectedMetadata = {};
  metadataKeys.forEach((key) => {
    if (selectedMetaKeys[key]) selectedMetadata[key] = metadata[key];
  });

  const exportSelectedMetadata = () => {
    const blob = new Blob([JSON.stringify(selectedMetadata, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${id}_metadata_selected.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="p-6 space-y-6">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-sm text-gray-500">
        <Link to="/sessions" className="hover:text-blue-600 transition-colors">Sessions</Link>
        <span>/</span>
        <span className="font-mono text-gray-700">{id}</span>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-3 text-red-700 text-sm font-mono">
          {error}
        </div>
      )}

      {/* Stats Grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="Frames" value={ts.frame_count?.toLocaleString()} />
        <StatCard label="Durée"  value={duration} />
        <StatCard label="Début"  value={ts.start_ts?.slice(11, 19)} />
        <StatCard label="Fin"    value={ts.end_ts?.slice(11, 19)} />
      </div>

      {/* Tracker averages */}
      {summary && (
        <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
          <p className="text-xs text-gray-400 uppercase tracking-wide mb-3">Position moyenne des trackers</p>
          <div className="grid grid-cols-3 gap-4 text-sm">
            {[1, 2, 3].map(n => (
              <div key={n} className="space-y-1">
                <p className="font-medium text-gray-600">Tracker {n}</p>
                {["x", "y", "z"].map(axis => (
                  <p key={axis} className="font-mono text-gray-500 text-xs">
                    {axis.toUpperCase()} : {ts[`avg_t${n}_${axis}`] ?? "—"}
                  </p>
                ))}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Pince stats */}
      {summary?.pince_stats?.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
          <p className="text-xs text-gray-400 uppercase tracking-wide mb-3">Statistiques pinces</p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {summary.pince_stats.map(p => (
              <div key={p.pince_id} className="bg-gray-50 rounded-lg p-3 text-sm">
                <p className="font-semibold text-gray-700 font-mono">{p.pince_id}</p>
                <p className="text-gray-500 text-xs mt-1">
                  Ouverture moy. <span className="font-medium text-gray-700">{p.avg_ouverture_mm} mm</span>
                  {" · "}Max <span className="font-medium text-gray-700">{p.max_ouverture_mm} mm</span>
                  {" · "}Angle moy. <span className="font-medium text-gray-700">{p.avg_angle_deg}°</span>
                </p>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <p className="text-xs text-gray-400 uppercase tracking-wide">Metadata JSON</p>
          <div className="flex flex-wrap items-center gap-2">
            <input
              value={metaQuery}
              onChange={(e) => setMetaQuery(e.target.value)}
              placeholder="Rechercher dans metadata..."
              className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm w-72"
            />
            <select
              value={metaScope}
              onChange={(e) => setMetaScope(e.target.value)}
              className="border border-gray-300 rounded-lg px-2 py-1.5 text-sm"
            >
              <option value="current">Cette session</option>
              <option value="all">Toutes les sessions</option>
            </select>
            <button
              type="button"
              onClick={() => {
                const next = {};
                metadataKeys.forEach((k) => { next[k] = true; });
                setSelectedMetaKeys(next);
              }}
              className="px-2.5 py-1.5 text-xs border border-gray-300 rounded-lg hover:bg-gray-50"
            >
              Tout sélectionner
            </button>
            <button
              type="button"
              onClick={() => {
                const next = {};
                metadataKeys.forEach((k) => { next[k] = false; });
                setSelectedMetaKeys(next);
              }}
              className="px-2.5 py-1.5 text-xs border border-gray-300 rounded-lg hover:bg-gray-50"
            >
              Tout retirer
            </button>
            <button
              type="button"
              onClick={exportSelectedMetadata}
              disabled={!metadata || Object.keys(selectedMetadata).length === 0}
              className="px-2.5 py-1.5 text-xs rounded-lg text-white bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300"
            >
              Export JSON sélectionné
            </button>
          </div>
        </div>

        {metadataError && (
          <p className="text-xs text-red-600 font-mono">{metadataError}</p>
        )}

        {metadata && (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            {filteredMetadataKeys.map((key) => (
              <div key={key} className="border border-gray-200 rounded-lg p-3 bg-gray-50">
                <label className="flex items-start gap-2">
                  <input
                    type="checkbox"
                    checked={!!selectedMetaKeys[key]}
                    onChange={(e) =>
                      setSelectedMetaKeys((prev) => ({ ...prev, [key]: e.target.checked }))
                    }
                    className="mt-0.5"
                  />
                  <div className="min-w-0">
                    <p className="font-mono text-xs text-gray-700 break-all">{key}</p>
                    <p className="text-xs text-gray-500 mt-1 break-words">
                      {summarizeMetadataValue(metadata[key])}
                    </p>
                    <details className="mt-2">
                      <summary className="cursor-pointer text-xs text-blue-600">Voir détail</summary>
                      <pre className="mt-1 text-xs bg-white p-2 rounded border border-gray-200 overflow-auto max-h-44">
                        {JSON.stringify(metadata[key], null, 2)}
                      </pre>
                    </details>
                  </div>
                </label>
              </div>
            ))}
            {filteredMetadataKeys.length === 0 && (
              <p className="text-xs text-gray-400">Aucun bloc metadata pour ce filtre.</p>
            )}
          </div>
        )}

        {metaScope === "all" && metaQuery.trim() && (
          <div className="border-t border-gray-100 pt-3">
            <p className="text-xs text-gray-500 mb-2">
              Résultats dans toutes les sessions ({globalMatches.length})
            </p>
            <div className="space-y-1">
              {globalMatches.map((row) => (
                <Link
                  key={row.session_dir}
                  to={`/sessions/${row.session_dir}`}
                  className="block text-sm font-mono text-blue-600 hover:underline"
                >
                  {row.session_dir}
                </Link>
              ))}
              {globalMatches.length === 0 && (
                <p className="text-xs text-gray-400">Aucun match.</p>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Tab Bar */}
      <div>
        <div className="flex gap-1 border-b border-gray-200 mb-4">
          {TABS.map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-5 py-2.5 text-sm font-medium rounded-t-lg transition-colors
                ${activeTab === tab
                  ? "bg-white border border-b-white border-gray-200 -mb-px text-blue-600"
                  : "text-gray-500 hover:text-gray-700"}`}
            >
              {tab}
            </button>
          ))}
        </div>

        {loading && (
          <div className="flex items-center gap-3 text-gray-400 py-8">
            <span className="animate-spin text-xl">⟳</span> Chargement…
          </div>
        )}

        {!loading && tableData.length > 0 && (
          <>
            <DataChart data={tableData} activeTab={activeTab} />
            {activeTab === "Tracker"
              ? <TrackerTable rows={tableData} />
              : <PinceTable   rows={tableData} />}
          </>
        )}

        {!loading && tableData.length === 0 && !error && (
          <p className="text-gray-400 text-sm py-4">Aucune donnée pour cet onglet.</p>
        )}
      </div>
    </div>
  );
}
