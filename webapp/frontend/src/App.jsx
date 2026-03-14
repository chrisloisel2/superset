import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Sidebar from "./components/layout/Sidebar";
import TopBar from "./components/layout/TopBar";
import SessionsPage from "./pages/SessionsPage";
import SessionDetailPage from "./pages/SessionDetailPage";
import QueryPage from "./pages/QueryPage";
import SessionsMetadataPage from "./pages/SessionsMetadataPage";

export default function App() {
  return (
    <BrowserRouter>
      <div className="flex h-screen bg-gray-50 overflow-hidden">
        <Sidebar />
        <div className="flex flex-col flex-1 overflow-hidden">
          <TopBar />
          <main className="flex-1 overflow-y-auto">
            <Routes>
              <Route path="/" element={<Navigate to="/sessions" replace />} />
              <Route path="/sessions" element={<SessionsPage />} />
              <Route path="/sessions-metadata" element={<SessionsMetadataPage />} />
              <Route path="/sessions/:id" element={<SessionDetailPage />} />
              <Route path="/query" element={<QueryPage />} />
            </Routes>
          </main>
        </div>
      </div>
    </BrowserRouter>
  );
}
