"""
Export metadata (schema, sample rows, stats, useful queries) for all databases.
Outputs data/db_metadata.json for the db-explorer page.
"""
import sqlite3
import json
import os

DATABASES = [
    ("policy-db", "Policy DB", "~/projects/apps/policy-db/data/policy.db",
     "全中央省庁の審議会・委員会・予算事業データベース",
     [
         ("テーマ別委員会数", "SELECT g.name AS core_theme, g.council_count, g.ministry_count FROM gta_core_themes g ORDER BY g.council_count DESC"),
         ("省庁別委員会数", "SELECT ministry_name, count(*) AS cnt FROM councils GROUP BY ministry_name ORDER BY cnt DESC"),
         ("有識者の兼任ランキング", "SELECT p.name, p.primary_org, count(DISTINCT a.council_id) AS appointments FROM persons p JOIN appointments a ON a.person_id = p.id GROUP BY p.id ORDER BY appointments DESC LIMIT 20"),
         ("予算事業トップ20", "SELECT name, ministry, finalized_amount FROM projects WHERE fiscal_year = 2025 ORDER BY finalized_amount DESC LIMIT 20"),
         ("GTA下位テーマ一覧", "SELECT s.name AS sub_theme, g.name AS core_theme, s.council_count FROM gta_sub_themes s JOIN gta_core_themes g ON g.id = s.core_theme_id ORDER BY s.council_count DESC LIMIT 30"),
     ]),
    ("sangaku-matcher", "Sangaku Matcher", "~/projects/apps/sangaku-matcher/data/matcher.db",
     "産学マッチャー（大学技術シーズ x 企業ニーズ）",
     [
         ("企業一覧（特許数順）", "SELECT name, industry, patent_count FROM companies ORDER BY patent_count DESC LIMIT 20"),
         ("技術シーズ一覧", "SELECT title, field, university FROM seeds LIMIT 20"),
         ("マッチング結果", "SELECT c.name AS company, s.title AS seed, m.score FROM matches m JOIN companies c ON c.id = m.company_id JOIN seeds s ON s.id = m.seed_id ORDER BY m.score DESC LIMIT 20"),
     ]),
    ("future-insight", "Future Insight", "~/projects/apps/future-insight-app/data/future_insight.db",
     "CLAフレームワークによる未来洞察",
     [
         ("コレクション一覧", "SELECT name, description, article_count FROM collections ORDER BY article_count DESC LIMIT 20"),
         ("最新記事", "SELECT title, source, published_at FROM articles ORDER BY published_at DESC LIMIT 20"),
         ("CLA分析", "SELECT title, litany, systemic_causes FROM cla_analyses LIMIT 10"),
         ("トレンド", "SELECT keyword, count, first_seen FROM trends ORDER BY count DESC LIMIT 20"),
     ]),
    ("ir-collector", "IR Collector", "~/projects/apps/ir-collector/data/ir.db",
     "上場企業IR情報（EDINET API）",
     [
         ("企業別文書数", "SELECT company_name, count(*) AS docs FROM documents GROUP BY company_name ORDER BY docs DESC LIMIT 20"),
         ("文書タイプ別", "SELECT doc_type, count(*) FROM documents GROUP BY doc_type ORDER BY count(*) DESC"),
         ("最新文書", "SELECT company_name, doc_type, filing_date FROM documents ORDER BY filing_date DESC LIMIT 20"),
     ]),
    ("mail-contacts", "Mail Contacts", "~/projects/apps/mail-contacts-app/emlx_index.db",
     "メール連絡先抽出・整理",
     [
         ("レコード数", "SELECT count(*) AS total FROM idx"),
         ("サンプル", "SELECT * FROM idx LIMIT 10"),
     ]),
    ("secretary-plaud", "Secretary App (Plaud)", "~/projects/apps/secretary-app/data/plaud.db",
     "音声録音・分析DB（Plaud連携）",
     [
         ("録音一覧", "SELECT id, title, duration, created_at FROM recordings ORDER BY created_at DESC LIMIT 20"),
     ]),
    ("newsletter", "Newsletter", "~/projects/apps/newsletter-app/data/newsletter.db",
     "ニュースレター配信管理",
     [
         ("配信リスト", "SELECT name, count FROM contact_lists ORDER BY count DESC"),
         ("キャンペーン", "SELECT subject, status, sent_at FROM campaigns ORDER BY sent_at DESC LIMIT 20"),
     ]),
    ("paper-database", "Paper Database", "~/projects/research/paper-database/papers.db",
     "学術論文DB（OpenAlex/Semantic Scholar/CiNii統合）",
     [
         ("論文一覧", "SELECT title, authors, year, source FROM papers ORDER BY year DESC LIMIT 20"),
     ]),
    ("essence-crm", "Essence CRM", "~/projects/apps/essence-crm/data/essence.db",
     "営業管理CRM",
     [
         ("組織一覧", "SELECT name, industry, stage FROM organizations LIMIT 20"),
         ("営業案件", "SELECT d.title, o.name AS org, ds.name AS stage FROM deals d JOIN organizations o ON o.id = d.organization_id JOIN deal_stages ds ON ds.id = d.stage_id ORDER BY d.updated_at DESC LIMIT 20"),
         ("セミナー一覧", "SELECT title, date, attendee_count FROM seminars ORDER BY date DESC LIMIT 10"),
     ]),
    ("researcher-profiler", "Researcher Profiler", "~/projects/apps/researcher-profiler/researcher.db",
     "研究者DB（Semantic Scholar連携）",
     [
         ("研究者一覧", "SELECT name, affiliation, h_index FROM researchers ORDER BY h_index DESC LIMIT 20"),
         ("論文一覧", "SELECT title, year, citation_count FROM papers ORDER BY citation_count DESC LIMIT 20"),
     ]),
    ("keyword-analyzer", "Keyword Analyzer", "~/projects/apps/keyword-analyzer/keywords.db",
     "検索キーワード分析",
     [
         ("キーワード一覧", "SELECT keyword, search_count FROM keywords ORDER BY search_count DESC LIMIT 20"),
     ]),
    ("qualitative-research", "Qualitative Research", "~/projects/apps/qualitative-research-tool/data/qualitative.db",
     "質的研究コーディング・分析ツール（GTA）",
     [
         ("プロジェクト", "SELECT name, methodology, created_at FROM projects LIMIT 10"),
         ("コード一覧", "SELECT name, category, frequency FROM codes ORDER BY frequency DESC LIMIT 20"),
     ]),
    ("speaker-manager", "Speaker Manager", "~/projects/apps/speaker-manager/data/speaker.db",
     "スピーカー・イベント管理",
     [
         ("スピーカー一覧", "SELECT name, title, organization FROM speakers LIMIT 20"),
         ("イベント一覧", "SELECT title, date, status FROM events ORDER BY date DESC LIMIT 10"),
     ]),
    ("organization-app", "Organization App", "~/projects/apps/organization-app/data/org.db",
     "AI組織運営ダッシュボード",
     [
         ("CxO議事録", "SELECT title, date, summary FROM cxo_minutes ORDER BY date DESC LIMIT 10"),
         ("PESTLEニュース", "SELECT title, category, published_at FROM pestle_articles ORDER BY published_at DESC LIMIT 10"),
     ]),
    ("miratuku-membership", "Miratuku Membership", "~/projects/apps/miratuku-membership/data/miratuku.db",
     "会員管理（Stripe決済）",
     [
         ("会員一覧", "SELECT email, plan, status FROM subscribers LIMIT 20"),
     ]),
    ("miratuku-events", "Miratuku Events", "~/projects/apps/miratuku-events/data/local.db",
     "イベント管理+オンデマンド学習",
     [
         ("イベント一覧", "SELECT title, date, status FROM events LIMIT 10"),
     ]),
    ("miratuku-team-hub", "Miratuku Team Hub", "~/projects/apps/miratuku-team-hub/data/local.db",
     "Slack/Notion統合チームハブ",
     [
         ("プロジェクト一覧", "SELECT name, status FROM projects LIMIT 10"),
         ("意思決定ログ", "SELECT title, decided_at FROM decisions ORDER BY decided_at DESC LIMIT 10"),
     ]),
    ("lecture-video-editor", "Lecture Video Editor", "~/projects/apps/lecture-video-editor/data/jobs.db",
     "講義動画自動編集パイプライン",
     [
         ("ジョブ一覧", "SELECT * FROM jobs ORDER BY rowid DESC LIMIT 10"),
     ]),
]

def export_db(db_id, name, path, desc, queries):
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        return None

    result = {
        "id": db_id,
        "name": name,
        "path": path.replace(os.path.expanduser("~"), "~"),
        "desc": desc,
        "size_mb": round(os.path.getsize(path) / (1024 * 1024), 1),
        "tables": [],
        "queries": [],
    }

    try:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        table_names = [r[0] for r in cursor.fetchall()]

        for tname in table_names:
            # Skip FTS internal tables
            if any(tname.endswith(s) for s in ['_data', '_idx', '_docsize', '_config', '_content']):
                continue

            table_info = {"name": tname, "columns": [], "row_count": 0, "sample_rows": []}

            try:
                cursor.execute(f"SELECT count(*) FROM [{tname}]")
                table_info["row_count"] = cursor.fetchone()[0]
            except:
                pass

            try:
                cursor.execute(f"PRAGMA table_info([{tname}])")
                cols = cursor.fetchall()
                table_info["columns"] = [
                    {"name": c["name"], "type": c["type"], "pk": bool(c["pk"]), "notnull": bool(c["notnull"])}
                    for c in cols
                ]
            except:
                pass

            # Sample rows (limit 5)
            try:
                cursor.execute(f"SELECT * FROM [{tname}] LIMIT 5")
                col_names = [d[0] for d in cursor.description]
                rows = cursor.fetchall()
                sample = []
                for row in rows:
                    r = {}
                    for i, col in enumerate(col_names):
                        val = row[i]
                        # Truncate long strings
                        if isinstance(val, str) and len(val) > 200:
                            val = val[:200] + "..."
                        elif isinstance(val, bytes):
                            val = f"<BLOB {len(val)} bytes>"
                        r[col] = val
                    sample.append(r)
                table_info["sample_rows"] = sample
            except:
                pass

            result["tables"].append(table_info)

        # Execute preset queries
        for qname, sql in queries:
            try:
                cursor.execute(sql)
                col_names = [d[0] for d in cursor.description]
                rows = cursor.fetchall()
                qresult = []
                for row in rows:
                    r = {}
                    for i, col in enumerate(col_names):
                        val = row[i]
                        if isinstance(val, str) and len(val) > 300:
                            val = val[:300] + "..."
                        elif isinstance(val, bytes):
                            val = f"<BLOB {len(val)} bytes>"
                        r[col] = val
                    qresult.append(r)
                result["queries"].append({"name": qname, "sql": sql, "columns": col_names, "rows": qresult})
            except Exception as e:
                result["queries"].append({"name": qname, "sql": sql, "error": str(e), "columns": [], "rows": []})

        conn.close()
    except Exception as e:
        result["error"] = str(e)

    return result


def main():
    all_data = []
    for db_id, name, path, desc, queries in DATABASES:
        print(f"Exporting {name}...")
        data = export_db(db_id, name, path, desc, queries)
        if data:
            all_data.append(data)
            print(f"  {len(data['tables'])} tables, {len(data['queries'])} queries")
        else:
            print(f"  SKIPPED (not found)")

    out_path = os.path.join(os.path.dirname(__file__), "..", "data", "db_metadata.json")
    with open(out_path, "w") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nWrote {out_path} ({len(all_data)} databases)")


if __name__ == "__main__":
    main()
