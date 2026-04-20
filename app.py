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



@app.route("/api/pdfs/search")
def search_pdf_titles():
    mandant = request.args.get("mandant", "").strip()
    query_raw = request.args.get("q", "").strip()
    form_id_param = request.args.get("form_id")
    year_param = request.args.get("year")

    limit = min(int(request.args.get("limit", 50)), 200)

    # SQL-Struktur
    sql = """
    SELECT pdf.*, form.name AS form_name, kontakt.full_name AS kontakt_name
    FROM pdf
    LEFT JOIN form ON pdf.form_id = form.id
    LEFT JOIN kontakt ON pdf.kontakt_id = kontakt.kontakt_id
    WHERE 1=1
    """
    params = []

    if mandant:
        sql += " AND mandant = ?"
        params.append(mandant)

    # 1. Filter: Form ID (direkt über Parameter)
    if form_id_param:
        sql += " AND pdf.form_id = ?"
        params.append(form_id_param)

    # 2. Filter: Erstellungsjahr (aus dem Feld 'created')
    if year_param:
        # Extrahiert das Jahr aus dem 'created' Datum (Format YYYY-MM-DD)
        sql += " AND strftime('%Y', pdf.created) = ?"
        params.append(str(year_param))

    # 3. Filter: Freitextsuche (falls vorhanden)
    if query_raw:
        pattern = f"%{query_raw}%"
        sql += """ 
        AND (COALESCE(pdf.title, '') LIKE ?
          OR COALESCE(pdf.id, '') LIKE ? 
          OR COALESCE(kontakt_name, '') LIKE ?
          OR COALESCE(pdf.keywords, '') LIKE ? 
          )
          """
        params.extend([pattern] * 4)

    sql += " ORDER BY pdf.created DESC LIMIT ?"
    params.append(limit)

    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    # ... (Rest der JSON-Aufbereitung wie gehabt)
    items = []
    for row in rows:
        # Hier greifen wir mit [key] zu, nicht mit .get()
        # Da der JOIN "kontakt" heißen könnte, prüfen wir kurz, ob der key existiert
        kontakt_name = row["kontakt_name"] if "kontakt_name" in row.keys() else "-"
        display_title = row["title"] if row["title"] else row["name"]

        items.append(
            {
                "id": row["id"],
                "title": display_title,
                "form_id": row["form_id"],
                "form_name": row["form_name"] or "Unbekannt",
                "created": row["created"],
                "kontakt_name": kontakt_name, 
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
