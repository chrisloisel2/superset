const COLS = [
  "frame_number", "timestamp",
  "tracker_1_x", "tracker_1_y", "tracker_1_z",
  "tracker_2_x", "tracker_2_y", "tracker_2_z",
  "tracker_3_x", "tracker_3_y", "tracker_3_z",
];

export default function TrackerTable({ rows }) {
  if (!rows?.length) return <p className="text-gray-400 text-sm py-4">Aucune donnée</p>;

  const keys = Object.keys(rows[0]);
  const display = keys.length <= 10 ? keys : COLS.filter(c => keys.includes(c));

  return (
    <div className="overflow-x-auto rounded-xl border border-gray-200 shadow-sm">
      <table className="w-full text-xs text-left">
        <thead className="bg-gray-50 text-gray-500 uppercase">
          <tr>
            {display.map(c => (
              <th key={c} className="px-3 py-2 whitespace-nowrap font-medium">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {rows.map((row, i) => (
            <tr key={i} className="hover:bg-blue-50 transition-colors">
              {display.map(c => (
                <td key={c} className="px-3 py-1.5 font-mono text-gray-600 whitespace-nowrap">
                  {row[c] != null ? String(row[c]) : "—"}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
