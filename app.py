print(">>> USING APP FILE:", __file__)
from pathlib import Path
import sqlite3

from flask import Flask, jsonify, request, abort, send_file

app = Flask(__name__)

DB_PATH = "/mnt/nas/data/adacta/db/adacta.db3"
PDF_BASE_DIR = Path("/mnt/nas/data/adacta/archiv").resolve()



def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "service": "adacta-api"})


@app.route("/api/forms")
def get_forms():
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, name
            FROM form
            ORDER BY name COLLATE NOCASE ASC
            """
        ).fetchall()
    finally:
        conn.close()

    items = [{"id": row["id"], "name": row["name"]} for row in rows]
    return jsonify({"items": items})


@app.route("/api/pdfs")
def list_pdfs():
    search = request.args.get("search", "").strip()
    mandant = request.args.get("mandant", "").strip()
    form_id = request.args.get("form_id", "").strip()
    limit = min(int(request.args.get("limit", 100)), 500)

    sql = """
    SELECT
        pdf.id,
        pdf.name,
        pdf.title,
        pdf.subject,
        pdf.keywords,
        pdf.author,
        pdf.creator,
        pdf.lastupdate,
        pdf.created,
        pdf.seiten,
        pdf.notizen,
        pdf.importname,
        pdf.mandant,
        pdf.status,
        pdf.form_id,
	pdf.kontakt_id,
        form.name AS form_name,
        kontakt.full_name AS kontakt_name
    FROM pdf
    LEFT JOIN form ON pdf.form_id = form.id
    LEFT JOIN kontakt ON pdf.kontakt_id = kontakt.kontakt_id
    WHERE 1 = 1
    """
    params = []

    if mandant:
        sql += " AND mandant = ?"
        params.append(mandant)

    if form_id:
        sql += " AND form_id = ?"
        params.append(form_id)

    if search:
        sql += """
        AND (
            COALESCE(pdf.id, '') LIKE ?
            OR COALESCE(pdf.name, '') LIKE ?
            OR COALESCE(pdf.title, '') LIKE ?
            OR COALESCE(pdf.subject, '') LIKE ?
            OR COALESCE(pdf.keywords, '') LIKE ?
            OR COALESCE(pdf.notizen, '') LIKE ?
            OR COALESCE(pdf.importname, '') LIKE ?
        )
        """
        pattern = f"%{search}%"
        params.extend([pattern] * 7)

    sql += " ORDER BY pdf.lastupdate DESC LIMIT ?"
    params.append(limit)

    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    items = []
    for row in rows:
        items.append(
            {
                "id": row["id"],
                "name": row["name"],
                "title": row["title"],
                "subject": row["subject"],
                "keywords": row["keywords"],
                "author": row["author"],
                "creator": row["creator"],
                "lastupdate": row["lastupdate"],
                "created": row["created"],
                "seiten": row["seiten"],
                "notizen": row["notizen"],
                "importname": row["importname"],
                "mandant": row["mandant"],
                "status": row["status"],
                "form_id": row["form_id"],
		"form_name": row["form_name"] or "Unbekannt",
        	"kontakt_id": row["kontakt_id"],
        	"kontakt_name": row["kontakt_name"],
                "pdf_url": f"/api/pdfs/by-id/{row['id']}/file",
            }
        )

    return jsonify({"count": len(items), "items": items})


@app.route("/api/pdfs/search")
def search_pdf_titles():
    query_raw = request.args.get("q", "").strip()
    query = query_raw.lower()
    limit = min(int(request.args.get("limit", 50)), 200)

    if not query_raw:
        return jsonify({"count": 0, "items": []})

    matched_form_id = None

    # 🔍 form_id erkennen
    if query.isdigit():
        form_id_candidate = int(query)
        if form_id_candidate in FORM_TYPES:
            matched_form_id = form_id_candidate
    else:
        for form_id, form_name in FORM_TYPES.items():
            if query == form_name.lower():
                matched_form_id = form_id
                break

    pattern = f"%{query_raw}%"

    # 🔥 SQL dynamisch bauen
    sql = """
    SELECT
        pdf.id,
        pdf.title,
        pdf.keywords,
        pdf.name,
        pdf.form_id,
        pdf.lastupdate,
        form.name AS form_name
    FROM pdf
    LEFT JOIN form ON pdf.form_id = form.id
    WHERE 1=1
    """

    params = []

    if matched_form_id is not None:
        # 🔥 UND-Verknüpfung
        sql += " AND form_id = ?"
        params.append(matched_form_id)

    # 🔍 Freitext immer zusätzlich
    sql += """
        AND (
            COALESCE(title, '') LIKE ?
            OR COALESCE(keywords, '') LIKE ?
        )
    """
    params.extend([pattern, pattern])

    sql += """
        ORDER BY
            COALESCE(pdf.title, pdf.name) COLLATE NOCASE ASC,
            pdf.lastupdate DESC
        LIMIT ?
    """
    params.append(limit)

    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    items = []
    for row in rows:
        display_title = row["title"] if row["title"] else row["name"]

        items.append(
            {
                "id": row["id"],
                "title": display_title,
                "form_id": row["form_id"],
		"form_name": row["form_name"] or "Unbekannt",
                "pdf_url": f"/api/pdfs/by-id/{row['id']}/file" 
	    }
        )

    return jsonify({"count": len(items), "items": items})


@app.route("/api/pdfs/by-id/<pdf_id>")
def get_pdf_metadata(pdf_id: str):
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT
                id,
                name,
                title,
                subject,
                keywords,
                author,
                creator,
                lastupdate,
                created,
                seiten,
                notizen,
                importname,
                mandant,
                status,
                form_id
            FROM pdf
            WHERE id = ?
            """,
            (pdf_id,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        abort(404, description="PDF metadata not found")

    return jsonify(
        {
            "id": row["id"],
            "name": row["name"],
            "title": row["title"],
            "subject": row["subject"],
            "keywords": row["keywords"],
            "author": row["author"],
            "creator": row["creator"],
            "lastupdate": row["lastupdate"],
            "created": row["created"],
            "seiten": row["seiten"],
            "notizen": row["notizen"],
            "importname": row["importname"],
            "mandant": row["mandant"],
            "status": row["status"],
            "form_id": row["form_id"],
            "form_name": FORM_TYPES.get(row["form_id"], "Unbekannt"),
            "pdf_url": f"/api/pdfs/by-id/{row['id']}/file",
        }
    )


@app.route("/api/pdfs/by-id/<pdf_id>/file")
def get_pdf_file(pdf_id: str):
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT id, name
            FROM pdf
            WHERE id = ?
            """,
            (pdf_id,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        abort(404, description="PDF not found in database")

    pdf_path = (PDF_BASE_DIR / row["name"]).resolve()

    try:
        pdf_path.relative_to(PDF_BASE_DIR)
    except ValueError:
        abort(400, description="Invalid file path")

    if not pdf_path.exists() or not pdf_path.is_file():
        abort(404, description=f"PDF file missing: {pdf_path}")

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=False,
        download_name=Path(row["name"]).name,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
