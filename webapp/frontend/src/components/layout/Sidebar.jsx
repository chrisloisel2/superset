import { NavLink } from "react-router-dom";

const NAV = [
  { to: "/sessions", label: "Sessions", icon: "🗂" },
  { to: "/sessions-metadata", label: "Métadonnées", icon: "🧾" },
  { to: "/query",    label: "Console HiveQL", icon: "⚡" },
];

export default function Sidebar() {
  return (
    <aside className="w-56 bg-gray-900 text-gray-100 flex flex-col shrink-0">
      <div className="px-5 py-5 border-b border-gray-700">
        <p className="text-xs text-gray-400 uppercase tracking-widest">HDFS Explorer</p>
        <p className="text-sm font-semibold mt-0.5">Session Viewer</p>
      </div>

      <nav className="flex-1 px-3 py-4 space-y-1">
        {NAV.map(({ to, label, icon }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors
               ${isActive
                 ? "bg-blue-600 text-white"
                 : "text-gray-400 hover:bg-gray-800 hover:text-white"
               }`
            }
          >
            <span>{icon}</span>
            {label}
          </NavLink>
        ))}
      </nav>

      <div className="px-5 py-3 border-t border-gray-700 text-xs text-gray-500">
        Hive · HDFS · Hadoop
      </div>
    </aside>
  );
}
