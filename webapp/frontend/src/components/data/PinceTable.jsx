const COLS = ["timestamp", "t_ms", "pince_id", "sw", "ouverture_mm", "angle_deg"];

const SW_BADGE = {
  ON:  "bg-green-100 text-green-700",
  OFF: "bg-gray-100  text-gray-500",
};

export default function PinceTable({ rows }) {
  if (!rows?.length) return <p className="text-gray-400 text-sm py-4">Aucune donnée</p>;

  const keys = Object.keys(rows[0]);
  const display = COLS.filter(c => keys.includes(c));

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
            <tr key={i} className="hover:bg-purple-50 transition-colors">
              {display.map(c => (
                <td key={c} className="px-3 py-1.5 font-mono text-gray-600 whitespace-nowrap">
                  {c === "sw" ? (
                    <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${SW_BADGE[row[c]] ?? "bg-gray-100 text-gray-500"}`}>
                      {row[c]}
                    </span>
                  ) : row[c] != null ? String(row[c]) : "—"}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
