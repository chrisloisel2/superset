import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { fetchSessions } from "../api/client";

export default function SessionsPage() {
  const [sessions, setSessions] = useState([]);
  const [loading, setLoading]   = useState(true);
  const [error, setError]       = useState(null);

  useEffect(() => {
    fetchSessions()
      .then((res) => {
        setSessions(res.data.sessions || []);
      })
      .catch(err => setError(err.response?.data?.error || err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="p-8 flex items-center gap-3 text-gray-500">
        <span className="animate-spin text-xl">⟳</span> Chargement des sessions…
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-8">
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700 text-sm font-mono">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-800">Sessions HDFS</h1>
        <span className="text-sm text-gray-400 bg-gray-100 px-3 py-1 rounded-full">
          {sessions.length} session{sessions.length !== 1 ? "s" : ""}
        </span>
      </div>

      {sessions.length === 0 ? (
        <div className="text-center py-16 text-gray-400">
          <p className="text-4xl mb-3">📂</p>
          <p className="font-medium">Aucune session trouvée</p>
          <p className="text-sm mt-1">Lancez le script <code className="bg-gray-100 px-1 rounded">register_sessions.py</code> pour enregistrer les sessions HDFS.</p>
        </div>
      ) : (
        <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
          <div className="px-4 py-3 bg-gray-50 border-b border-gray-200 text-[11px] uppercase tracking-wide text-gray-500">
            Session
          </div>
          {sessions.map((s) => {
            return (
              <Link
                key={s.session_id}
                to={`/sessions/${s.session_id}`}
                className="block px-4 py-3 border-b last:border-b-0 border-gray-100 hover:bg-slate-50 transition-colors"
              >
                <p className="font-mono text-sm text-gray-700 truncate">{s.session_id}</p>
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}
