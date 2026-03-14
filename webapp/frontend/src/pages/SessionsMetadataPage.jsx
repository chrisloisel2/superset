import { useEffect, useMemo, useState } from "react";
import { fetchSessionsMetadata } from "../api/client";

function isIsoDateString(value) {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value);
}

function formatScalar(value) {
  if (value === null || value === undefined) return "—";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return Number.isInteger(value) ? `${value}` : value.toFixed(3);
  if (typeof value === "string") {
    if (!isIsoDateString(value)) return value;
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? value : d.toLocaleString("fr-FR");
  }
  return String(value);
}

function BoolDot({ value }) {
  return (
    <span className="inline-flex items-center gap-2">
      <span
        className={`inline-block w-2.5 h-2.5 rounded-full ${
          value ? "bg-emerald-500 shadow-[0_0_0_3px_rgba(16,185,129,0.18)]" : "bg-rose-500 shadow-[0_0_0_3px_rgba(244,63,94,0.18)]"
        }`}
      />
      <span className={`text-xs font-semibold ${value ? "text-emerald-700" : "text-rose-700"}`}>
        {value ? "true" : "false"}
      </span>
    </span>
  );
}

function ValueCard({ label, value, depth = 0 }) {
  const nestedClass =
    depth === 0
      ? "bg-gradient-to-b from-white to-slate-50"
      : "bg-white";
  const cardPad = depth === 0 ? "p-3" : "p-2";

  if (value === null || value === undefined || typeof value !== "object") {
    return (
      <div className={`rounded-xl border border-slate-200 ${cardPad} ${nestedClass} w-full h-full shadow-sm`}>
        <p className="text-[10px] uppercase tracking-wide text-slate-500">{label}</p>
        <div className="mt-1 break-words">
          {typeof value === "boolean" ? (
            <BoolDot value={value} />
          ) : (
            <p className="text-xs font-mono text-slate-700">{formatScalar(value)}</p>
          )}
        </div>
      </div>
    );
  }

  const entries = Array.isArray(value)
    ? value.map((v, i) => [String(i), v])
    : Object.entries(value);

  return (
    <div className={`rounded-xl border border-slate-200 ${cardPad} ${nestedClass} w-full h-full shadow-sm`}>
      <p className="text-[10px] uppercase tracking-wide text-slate-500">
        {label} {Array.isArray(value) ? `[${value.length}]` : `{${entries.length}}`}
      </p>
      {entries.length === 0 ? (
        <p className="text-xs font-mono text-slate-400 mt-1">vide</p>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-2 mt-2">
          {entries.map(([k, v]) => (
            <ValueCard key={k} label={k} value={v} depth={depth + 1} />
          ))}
        </div>
      )}
    </div>
  );
}

export default function SessionsMetadataPage() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [query, setQuery] = useState("");
  const [selectedSessionDir, setSelectedSessionDir] = useState(null);

  useEffect(() => {
    fetchSessionsMetadata()
      .then((res) => {
        setRows(res.data.sessions || []);
      })
      .catch((e) => setError(e.response?.data?.error || e.message))
      .finally(() => setLoading(false));
  }, []);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return rows;

    return rows.filter((row) => {
      if (!row.metadata) {
        return `${row.session_dir} ${row.metadata_error || ""}`.toLowerCase().includes(q);
      }
      return `${row.session_dir} ${JSON.stringify(row.metadata)}`.toLowerCase().includes(q);
    });
  }, [rows, query]);

  const selectedRow = useMemo(
    () => filtered.find((r) => r.session_dir === selectedSessionDir) || null,
    [filtered, selectedSessionDir]
  );

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-800">Métadonnées JSON Sessions</h1>
        <span className="text-sm text-gray-500 bg-gray-100 px-3 py-1 rounded-full">
          {filtered.length} / {rows.length}
        </span>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-4">
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Filtrer dans metadata.json (clé/valeur)"
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm"
        />
      </div>

      {loading && <p className="text-gray-500">Chargement des metadata.json...</p>}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-3 text-red-700 text-sm font-mono">
          {error}
        </div>
      )}

      {!loading && !error && (
        <div className="space-y-4">
          <div className="bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm">
            {filtered.map((row) => (
              <button
                key={row.session_dir}
                type="button"
                onClick={() => setSelectedSessionDir(row.session_dir)}
                className="w-full text-left px-4 py-3 border-b border-gray-100 last:border-b-0 hover:bg-slate-50 transition-colors"
              >
                <p className="text-sm font-mono text-gray-700">{row.session_dir}</p>
              </button>
            ))}
          </div>

          {filtered.length === 0 && (
            <div className="bg-white border border-gray-200 rounded-xl p-6 text-gray-400">
              Aucun résultat.
            </div>
          )}
        </div>
      )}

      {selectedRow && (
        <div
          className="fixed inset-0 z-50 bg-black/35 flex items-center justify-center p-4"
          onClick={() => setSelectedSessionDir(null)}
        >
          <div
            className="bg-white border border-slate-200 rounded-2xl shadow-2xl w-full max-w-6xl max-h-[85vh] overflow-auto"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="sticky top-0 bg-white/95 backdrop-blur border-b border-slate-200 px-4 py-3 flex items-center justify-between">
              <p className="text-sm font-mono text-gray-700">{selectedRow.session_dir}</p>
              <button
                type="button"
                onClick={() => setSelectedSessionDir(null)}
                className="w-7 h-7 rounded-md border border-slate-300 text-slate-600 hover:bg-slate-100"
              >
                ✕
              </button>
            </div>

            <div className="p-4">
              {selectedRow.metadata ? (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 items-stretch">
                  {Object.entries(selectedRow.metadata).map(([key, value]) => (
                    <ValueCard key={key} label={key} value={value} />
                  ))}
                </div>
              ) : (
                <p className="text-xs text-red-600">{selectedRow.metadata_error || "metadata.json manquant"}</p>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
