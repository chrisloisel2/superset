import axios from "axios";

const api = axios.create({
  baseURL: "",   // same origin — nginx proxies /api/ to flask
  timeout: 60000,
});

export const fetchSessions  = ()             => api.get("/api/sessions");
export const fetchSession   = (id)           => api.get(`/api/sessions/${id}`);
export const fetchTracker   = (id, params)   => api.get(`/api/sessions/${id}/tracker`, { params });
export const fetchPince     = (id, num, p)   => api.get(`/api/sessions/${id}/pince${num}`, { params: p });
export const fetchSessionsMetadata = ()      => api.get("/api/metadata/sessions");
export const fetchSessionMetadata = (id)     => api.get(`/api/metadata/sessions/${id}`);
export const runQuery       = (sql)          => api.post("/api/query", { sql });
export const checkHealth    = ()             => api.get("/api/health");
