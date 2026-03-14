import { useEffect, useState } from "react";
import { checkHealth } from "../../api/client";

export default function TopBar() {
  const [online, setOnline] = useState(null);

  useEffect(() => {
    let stopped = false;

    const probe = () => {
      checkHealth()
        .then(() => {
          if (!stopped) setOnline(true);
        })
        .catch(() => {
          if (!stopped) setOnline(false);
        });
    };

    probe();
    const interval = setInterval(probe, 10000);
    const onVisible = () => {
      if (document.visibilityState === "visible") probe();
    };
    document.addEventListener("visibilitychange", onVisible);

    return () => {
      stopped = true;
      clearInterval(interval);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, []);

  return (
    <header className="h-12 bg-white border-b border-gray-200 flex items-center justify-between px-6 shrink-0">
      <span className="text-sm text-gray-500 font-mono">
        hdfs://namenode:9000/sessions
      </span>
      <div className="flex items-center gap-2 text-xs">
        <span
          className={`w-2 h-2 rounded-full ${
            online === null ? "bg-gray-300" :
            online ? "bg-green-500" : "bg-red-400"
          }`}
        />
        <span className="text-gray-500">
          {online === null ? "..." : online ? "API connectée" : "API hors ligne"}
        </span>
      </div>
    </header>
  );
}
