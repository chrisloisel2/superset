import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";

const TRACKER_LINES = [
  { key: "tracker_1_x", color: "#3b82f6" },
  { key: "tracker_1_y", color: "#10b981" },
  { key: "tracker_1_z", color: "#f59e0b" },
];

const PINCE_LINES = [
  { key: "ouverture_mm", color: "#6366f1" },
  { key: "angle_deg",    color: "#ec4899" },
];

export default function DataChart({ data, activeTab }) {
  if (!data?.length) return null;

  const isTracker = activeTab === "Tracker";
  const lines = isTracker ? TRACKER_LINES : PINCE_LINES;
  const xKey  = isTracker ? "frame_number" : "t_ms";

  const availableKeys = Object.keys(data[0]);
  const visibleLines  = lines.filter(l => availableKeys.includes(l.key));

  if (!visibleLines.length) return null;

  return (
    <div className="bg-white border border-gray-100 rounded-xl p-4 mb-4 shadow-sm">
      <p className="text-xs text-gray-400 mb-2 font-medium uppercase tracking-wide">
        {isTracker ? "Position Tracker 1 (x / y / z)" : "Pince — ouverture & angle"}
      </p>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey={xKey} tick={{ fontSize: 9 }} />
          <YAxis tick={{ fontSize: 9 }} width={50} />
          <Tooltip contentStyle={{ fontSize: "11px" }} />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          {visibleLines.map(l => (
            <Line
              key={l.key}
              type="monotone"
              dataKey={l.key}
              stroke={l.color}
              dot={false}
              strokeWidth={1.5}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
