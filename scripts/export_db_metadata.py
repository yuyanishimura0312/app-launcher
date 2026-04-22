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
    ("anthropology-concepts", "Anthropology Concepts", "~/projects/research/anthropology-concepts/data/anthropology.db",
     "人類学概念系譜DB（概念・研究者・文献・OCM分類）",
     [
         ("概念一覧", "SELECT name, field, era FROM concepts ORDER BY name LIMIT 20"),
         ("研究者一覧", "SELECT name, nationality, birth_year FROM researchers ORDER BY name LIMIT 20"),
     ]),
    ("funding-database", "Funding Database", "~/projects/research/investment-signal-radar/data/funding_database.db",
     "VC資金調達DB（企業・ラウンド・月次統計）",
     [
         ("最新ラウンド", "SELECT company_name, round_type, amount_jpy FROM funding_rounds ORDER BY announced_date DESC LIMIT 20"),
         ("月次統計", "SELECT month, total_amount, deal_count FROM monthly_stats ORDER BY month DESC LIMIT 12"),
     ]),
    ("sangaku-press-releases", "Sangaku Press Releases", "~/projects/research/investment-signal-radar/data/sangaku_press_releases.db",
     "産学連携プレスリリースDB（sangaku-matcher-v2由来）",
     [
         ("プレスリリース一覧", "SELECT title, organization, published_date FROM press_releases ORDER BY published_date DESC LIMIT 20"),
     ]),
    ("prtimes", "PR Times", "~/projects/apps/sangaku-matcher-v2/data/prtimes.db",
     "PR Timesプレスリリース・企業・大学連携データ",
     [
         ("プレスリリース", "SELECT title, company_name, published_at FROM press_releases ORDER BY published_at DESC LIMIT 20"),
         ("企業一覧", "SELECT name, industry FROM companies LIMIT 20"),
     ]),
    ("sangaku-matcher-v2", "Sangaku Matcher v2", "~/projects/apps/sangaku-matcher-v2/data/sangaku_v2.db",
     "産学マッチングv2（企業・シーズ・協業・特許・プレスリリース統合）",
     [
         ("企業一覧", "SELECT name, industry FROM companies ORDER BY name LIMIT 20"),
         ("技術シーズ", "SELECT title, field, university FROM seeds LIMIT 20"),
         ("マッチング結果", "SELECT company_id, seed_id, score FROM matches ORDER BY score DESC LIMIT 20"),
     ]),
    ("grant-db", "Grant DB", "~/projects/apps/grant-db/grant_db.sqlite",
     "助成金・補助金DB（6,113件・1,307機関・PDF解析・申請書テンプレ）",
     [
         ("助成金プログラム", "SELECT name, organization_name, category FROM grant_programs LIMIT 20"),
         ("公募一覧", "SELECT title, status, deadline FROM grant_calls ORDER BY deadline DESC LIMIT 20"),
         ("機関一覧", "SELECT name, type, grant_count FROM organizations ORDER BY grant_count DESC LIMIT 20"),
     ]),
    ("investment-signal-v2", "Investment Signal v2", "~/projects/research/investment-signal-radar/data/investment_signal_v2.db",
     "投資シグナルレーダーv2（組織・資金調達ラウンド・イベント・セクター）",
     [
         ("組織一覧", "SELECT name, type, sector FROM organizations LIMIT 20"),
         ("資金調達ラウンド", "SELECT organization_id, round_type, amount FROM funding_rounds ORDER BY announced_date DESC LIMIT 20"),
         ("イベント", "SELECT title, event_type, date FROM events ORDER BY date DESC LIMIT 20"),
     ]),
    ("experts-db", "Experts DB", "~/projects/research/experts-db/data/experts.db",
     "有識者ネットワークDB（省庁審議会・委員兼任・構造的空隙分析）",
     [
         ("有識者ランキング", "SELECT p.name, p.primary_org, count(DISTINCT a.council_id) AS cnt FROM persons p JOIN appointments a ON a.person_id = p.id GROUP BY p.id ORDER BY cnt DESC LIMIT 20"),
         ("橋渡し人物", "SELECT name, betweenness, communities FROM bridge_persons ORDER BY betweenness DESC LIMIT 20"),
     ]),
    ("pestle", "PESTLE Articles", "~/projects/apps/secretary-app/pestle.db",
     "PESTLEニュース記事DB（政治・経済・社会・技術・法律・環境）",
     [
         ("最新記事", "SELECT title, category, published_at FROM pestle_articles ORDER BY published_at DESC LIMIT 20"),
     ]),
    ("frontier-detector", "Frontier Detector", "~/projects/research/frontier-detector/frontier_detector.db",
     "学術フロンティア検出（技術・シグナル・研究者・スコア）",
     [
         ("技術一覧", "SELECT name, field, maturity FROM technologies LIMIT 20"),
         ("シグナル", "SELECT title, signal_type, relevance_score FROM signals ORDER BY relevance_score DESC LIMIT 20"),
     ]),
    ("healthy-aging", "Healthy Aging DB", "~/projects/research/healthy-aging-db/healthy_aging.db",
     "健康長寿研究DB（要因・介入・エビデンス・理論枠組み）",
     [
         ("要因一覧", "SELECT name, category, importance FROM factors ORDER BY importance DESC LIMIT 20"),
         ("介入一覧", "SELECT name, target, evidence_level FROM interventions LIMIT 20"),
     ]),
    ("structural-inflection", "Structural Inflection Radar", "~/projects/research/structural-inflection-radar/data/radar.db",
     "構造的変化点検出レーダー（ドメイン・キーワード・スコアリング）",
     [
         ("ドメイン一覧", "SELECT name, description FROM domains LIMIT 20"),
         ("キーワード", "SELECT keyword, domain_id FROM domain_keywords LIMIT 20"),
     ]),
    ("kakenhi-writer", "KAKENHI Writer", "~/projects/apps/kakenhi-writer/data/kakenhi.db",
     "科研費申請書自動生成（カテゴリ・評価基準・テンプレート）",
     [
         ("カテゴリ", "SELECT name, code FROM kakenhi_categories LIMIT 20"),
         ("審査区分", "SELECT name, category FROM review_sections LIMIT 20"),
     ]),
    ("basho-db", "Basho DB", "~/projects/apps/basho-db/data/basho.db",
     "場所性マッチング（飲食店・宿泊施設の場所性8軸評価）",
     [
         ("施設一覧", "SELECT name, type, prefecture FROM places LIMIT 20"),
     ]),
    ("great-figures-db", "Great Figures DB", "~/projects/research/great-figures-db/great_figures.db",
     "歴史偉人DB（9,178人・568概念・1,050イベント・741リンク）",
     [
         ("偉人一覧", "SELECT name, birth_year, field FROM figures ORDER BY birth_year LIMIT 20"),
         ("概念一覧", "SELECT name, category FROM concepts LIMIT 20"),
     ]),
    ("foresight-kb", "Foresight Knowledge Base", "~/projects/research/foresight-knowledge-base/foresight.db",
     "未来洞察ナレッジベース（CLA分析・シナリオ・シグナル統合）",
     [
         ("記事一覧", "SELECT title, source, published_at FROM articles ORDER BY published_at DESC LIMIT 20"),
     ]),
    ("academic-knowledge-db", "Academic Knowledge DB", "~/projects/research/academic-knowledge-db/academic.db",
     "5分野学術知識DB（人文1,203・社会851・自然1,761・工学420・芸術468）",
     [
         ("人文学概念", "SELECT name, field, era FROM humanities_concept LIMIT 20"),
         ("社会科学理論", "SELECT name, field, era FROM social_theory LIMIT 20"),
         ("自然科学発見", "SELECT name, field, era FROM natural_discovery LIMIT 20"),
     ]),
    ("lunar-life-db", "Lunar Life DB", "~/projects/research/lunar-life-db/data/lunar_life.db",
     "月面生活リサーチDB（JAXA資料ベース・10カテゴリ）",
     [
         ("課題一覧", "SELECT title, category FROM challenges LIMIT 20"),
     ]),
    ("writing-craft-db", "Writing Craft DB", "~/projects/research/writing-craft-db/writing_craft.db",
     "文章技法DB（288件・100技法・20巨匠・30メディア）",
     [
         ("技法一覧", "SELECT name, category FROM techniques LIMIT 20"),
     ]),
    ("yokai-sns", "Yokai SNS", "~/projects/apps/yokai-sns/data/yokai_utterances.db",
     "妖怪百景 ── 1,010体の妖怪が自律的に対話するSNS",
     [
         ("発言一覧", "SELECT * FROM utterances LIMIT 10"),
     ]),
    ("human-activities-db", "Human Activities DB", "~/projects/research/human-activities-db/data/human_activities.db",
     "人間活動データベース（活動分類・地域・時代）",
     [
         ("活動一覧", "SELECT name, category FROM activities LIMIT 20"),
     ]),
    ("pestle-pestle", "PESTLE Signal DB", "~/projects/research/pestle-signal-db/data/pestle.db",
     "PESTLEニュース構造化DB（毎日6時自動収集・6カテゴリ）",
     [
         ("最新記事", "SELECT title, category, published_at FROM articles ORDER BY published_at DESC LIMIT 20"),
     ]),
    ("cla-db", "CLA DB", "~/projects/research/pestle-signal-db/data/cla.db",
     "因果階層分析DB（36年分年次データ・22四半期・パラダイムシフト）",
     [
         ("年次分析", "SELECT year, title FROM annual_analyses ORDER BY year DESC LIMIT 20"),
     ]),
    ("signal-db", "Signal DB", "~/projects/research/pestle-signal-db/data/signal.db",
     "弱いシグナル検出DB（アラート・シナリオ・PESTLE統合）",
     [
         ("シグナル一覧", "SELECT title, category, score FROM signals ORDER BY score DESC LIMIT 20"),
     ]),
    ("academic-pressure", "Academic Pressure Detector", "~/projects/research/academic-pressure-detector/academic_pressure.db",
     "学術界の構造的圧力検出（研究動向・圧力源・影響分析）",
     [
         ("圧力一覧", "SELECT name, type FROM pressures LIMIT 20"),
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
