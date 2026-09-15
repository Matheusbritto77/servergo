package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"server-web/broker"
)

type WebServer struct {
	broker *broker.Broker
}

func NewWebServer(b *broker.Broker) *WebServer {
	return &WebServer{broker: b}
}

func (ws *WebServer) Start(addr string) error {
	mux := http.NewServeMux()

	downloadsDir := "./downloads"
	_ = os.MkdirAll(downloadsDir, 0755)
	fileServer := http.FileServer(http.Dir(downloadsDir))
	mux.Handle("/downloads/", http.StripPrefix("/downloads/", fileServer))

	mux.HandleFunc("/api/clients", ws.handleAPIClients)
	mux.HandleFunc("/api/update/check", ws.handleUpdateCheck)
	mux.HandleFunc("/", ws.handleDashboard)

	return http.ListenAndServe(addr, mux)
}

func (ws *WebServer) handleAPIClients(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	clients := ws.broker.ListClients()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"clients": clients,
		"count":   len(clients),
	})
}

func (ws *WebServer) handleUpdateCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	component := r.URL.Query().Get("component")
	currentVersion := r.URL.Query().Get("version")

	latestVersion := "0.1.0"
	updateAvailable := currentVersion != "" && currentVersion != latestVersion

	ext := ""
	if r.URL.Query().Get("os") == "windows" {
		ext = ".exe"
	}

	downloadURL := fmt.Sprintf("http://209.126.81.68:8090/downloads/remote-%s%s", component, ext)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"update_available": updateAvailable,
		"latest_version":   latestVersion,
		"download_url":     downloadURL,
	})
}

func (ws *WebServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AnyDesk Remote Support - Management Broker</title>
    <style>
        :root {
            --bg: #0f172a;
            --card-bg: #1e293b;
            --text: #f8fafc;
            --text-muted: #94a3b8;
            --accent: #38bdf8;
            --accent-glow: rgba(56, 189, 248, 0.2);
            --success: #4ade80;
            --warning: #facc15;
            --border: #334155;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { background-color: var(--bg); color: var(--text); padding: 2rem; min-height: 100vh; }
        .container { max-width: 1000px; margin: 0 auto; }
        header { display: flex; align-items: center; justify-content: space-between; padding-bottom: 1.5rem; border-bottom: 1px solid var(--border); margin-bottom: 2rem; }
        .logo { font-size: 1.5rem; font-weight: 700; background: linear-gradient(135deg, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .badge { background-color: var(--card-bg); border: 1px solid var(--border); padding: 0.4rem 0.8rem; border-radius: 9999px; font-size: 0.85rem; color: var(--text-muted); }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 1.5rem; margin-bottom: 2rem; }
        .card { background-color: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 1.5rem; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
        .card-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 1rem; color: var(--accent); }
        .stat-val { font-size: 2.5rem; font-weight: 800; color: #fff; }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid var(--border); }
        th { font-size: 0.85rem; text-transform: uppercase; color: var(--text-muted); }
        .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: var(--success); margin-right: 6px; }
        .status-dot.active { background-color: var(--warning); box-shadow: 0 0 8px var(--warning); }
        .empty-state { text-align: center; padding: 3rem 1rem; color: var(--text-muted); }
        code { background: #334155; padding: 2px 6px; border-radius: 4px; font-family: monospace; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo">⚡ Remote Desktop Broker</div>
            <div class="badge">gRPC: :50051 | Web: :8090 | Auto-Updater Ready</div>
        </header>

        <div class="grid">
            <div class="card">
                <div class="card-title">Active Host Clients</div>
                <div class="stat-val" id="client-count">0</div>
            </div>
            <div class="card">
                <div class="card-title">System Status</div>
                <div style="margin-top: 0.5rem; font-weight: 600; color: var(--success);">● Broker & Auto-Updater Active</div>
                <div style="font-size: 0.85rem; color: var(--text-muted); margin-top: 0.4rem;">Serving client and control binaries</div>
            </div>
        </div>

        <div class="card">
            <div class="card-title">Connected Remote Machines</div>
            <table>
                <thead>
                    <tr>
                        <th>Status</th>
                        <th>Client ID</th>
                        <th>Machine Name</th>
                        <th>OS Info</th>
                        <th>Registered At</th>
                    </tr>
                </thead>
                <tbody id="clients-body">
                    <tr><td colspan="5" class="empty-state">Loading registered client hosts...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        async function fetchClients() {
            try {
                const res = await fetch('/api/clients');
                const data = await res.json();
                document.getElementById('client-count').innerText = data.count || 0;
                
                const tbody = document.getElementById('clients-body');
                if (!data.clients || data.clients.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No client hosts currently registered. Start <code>remote-client.exe</code> on a Windows machine.</td></tr>';
                    return;
                }
                
                tbody.innerHTML = data.clients.map(function(c) {
                    var statusDot = c.has_control ? 'status-dot active' : 'status-dot';
                    var statusText = c.has_control ? 'In Remote Session' : 'Idle / Ready';
                    var mName = c.machine_name || 'N/A';
                    var os = c.os_info || 'Windows';
                    var timeStr = new Date(c.registered_at).toLocaleTimeString();
                    return '<tr>' +
                        '<td><span class="' + statusDot + '"></span>' + statusText + '</td>' +
                        '<td><strong>' + c.client_id + '</strong></td>' +
                        '<td>' + mName + '</td>' +
                        '<td>' + os + '</td>' +
                        '<td>' + timeStr + '</td>' +
                    '</tr>';
                }).join('');
            } catch (err) {
                console.error(err);
            }
        }
        fetchClients();
        setInterval(fetchClients, 3000);
    </script>
</body>
</html>`
	fmt.Fprint(w, html)
}
