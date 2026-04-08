# App Launcher

Quick-access dashboard for all Claude Code apps. Single HTML file, no dependencies.

## Usage

```bash
open ~/app-launcher/index.html
```

## Features

- One-click access to all web apps (Vercel / GitHub Pages)
- Copy-to-clipboard for local app launch commands
- Admin panel links with credentials
- Search filtering (Cmd+K to focus)
- **Dev reminder**: per-app "Next Action" + RAG color (recent / 7-30d / 30d+) + JSON export-import. Click any "+ 次のアクションを記録" on a card to start. Pull-only reminder (no notifications). Stored in localStorage `app-launcher:dev-state`. See `/tmp/best-practice-report.md` and Research Dashboard for the design rationale.

## Open via GitHub Pages (recommended)

Open `https://yuyanishimura0312.github.io/app-launcher/` and bookmark it. The dev reminder data is stored in localStorage tied to that origin. Avoid opening the local `file://` HTML directly because the storage origin will be different and your dev notes will not be visible from both routes.

## Backup

Click "JSON エクスポート" in the settings bar at the bottom to back up your dev state. Safari clears storage for sites unused for 7 days, so periodic export is recommended.
