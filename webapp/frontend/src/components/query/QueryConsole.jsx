import { useState } from "react";
import { runQuery } from "../../api/client";

const DEFAULT_SQL = `SELECT session_id, COUNT(*) AS frames
FROM robotics.tracker_positions
GROUP BY session_id
ORDER BY session_id DESC`;

const EXAMPLES = [
  {
    label: "Sessions disponibles",
    sql: `SELECT session_id, COUNT(*) AS frames\nFROM robotics.tracker_positions\nGROUP BY session_id\nORDER BY session_id DESC`,
  },
  {
    label: "Stats tracker session",
    sql: `SELECT\n  ROUND(AVG(tracker_1_x), 4) AS avg_x,\n  ROUND(AVG(tracker_1_y), 4) AS avg_y,\n  ROUND(AVG(tracker_1_z), 4) AS avg_z,\n  COUNT(*) AS frames\nFROM robotics.tracker_positions\nWHERE session_id = 'session_20260222_175519'`,
  },
  {
    label: "Stats pince par session",
    sql: `SELECT\n  session_id,\n  pince_id,\n  ROUND(AVG(ouverture_mm), 2) AS avg_ouverture,\n  ROUND(MAX(ouverture_mm), 2) AS max_ouverture\nFROM robotics.pince1_data\nGROUP BY session_id, pince_id\nORDER BY session_id DESC`,
  },
  {
    label: "Tables disponibles",
    sql: `SHOW TABLES IN robotics`,
  },
];

export default function QueryConsole() {
  const [sql, setSql]         = useState(DEFAULT_SQL);
  const [rows, setRows]       = useState([]);
  const [cols, setCols]       = useState([]);
  const [error, setError]     = useState(null);
  const [loading, setLoading] = useState(false);
  const [elapsed, setElapsed] = useState(null);

  const execute = async () => {
    setLoading(true);
    setError(null);
    setRows([]);
    const t0 = Date.now();
    try {
      const res  = await runQuery(sql);
      const data = res.data.data;
      setRows(data);
      setCols(data.length ? Object.keys(data[0]) : []);
      setElapsed(Date.now() - t0);
    } catch (e) {
      setError(e.response?.data?.error || e.message);
    } finally {
      setLoading(false);
    }
  };

  const handleKey = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") execute();
  };

  return (
    <div className="p-6 space-y-4 h-full flex flex-col">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-800">Console HiveQL</h1>
        <span className="text-xs text-gray-400">Ctrl+Entrée pour exécuter</span>
      </div>

      <div className="flex flex-wrap gap-2">
        {EXAMPLES.map(ex => (
          <button
            key={ex.label}
            onClick={() => setSql(ex.sql)}
            className="text-xs bg-gray-100 hover:bg-blue-100 hover:text-blue-700
                       text-gray-600 px-3 py-1 rounded-full transition-colors"
          >
            {ex.label}
          </button>
        ))}
      </div>

      <textarea
        value={sql}
        onChange={e => setSql(e.target.value)}
        onKeyDown={handleKey}
        rows={7}
        spellCheck={false}
        className="w-full font-mono text-sm border border-gray-300 rounded-xl p-4
                   focus:outline-none focus:ring-2 focus:ring-blue-400 resize-y bg-gray-900
                   text-green-400 placeholder-gray-600"
        placeholder="Entrez votre requête HiveQL..."
      />

      <div className="flex items-center gap-4">
        <button
          onClick={execute}
          disabled={loading}
          className="bg-blue-600 hover:bg-blue-700 text-white font-medium px-8 py-2.5
                     rounded-xl transition-colors disabled:opacity-50 flex items-center gap-2"
        >
          {loading ? (
            <>
              <span className="animate-spin text-base">⟳</span> Exécution…
            </>
          ) : "▶  Exécuter"}
        </button>
        {elapsed != null && !loading && (
          <span className="text-xs text-gray-400">{elapsed} ms · {rows.length} ligne(s)</span>
        )}
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 text-sm font-mono">
          {error}
        </div>
      )}

      {rows.length > 0 && (
        <div className="flex-1 overflow-auto rounded-xl border border-gray-200 shadow-sm">
          <table className="w-full text-sm text-left">
            <thead className="bg-gray-50 text-gray-500 uppercase text-xs sticky top-0">
              <tr>
                {cols.map(c => (
                  <th key={c} className="px-4 py-3 font-medium whitespace-nowrap">{c}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {rows.map((row, i) => (
                <tr key={i} className="hover:bg-blue-50 transition-colors">
                  {cols.map(c => (
                    <td key={c} className="px-4 py-2 font-mono text-gray-600 whitespace-nowrap">
                      {row[c] != null ? String(row[c]) : <span className="text-gray-300">null</span>}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
