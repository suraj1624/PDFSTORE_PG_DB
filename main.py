from fastapi import FastAPI, HTTPException, Form
from datetime import datetime
import pdfplumber
import psycopg2
import requests
import tempfile
import asyncio
import aiohttp  

app = FastAPI()

db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Radhekrishna@24",
    "host": "localhost",
    "port": 5432
}

async def extract_pdf_text_from_url_async(pdf_url: str) -> str:
    """
    Asynchronously downloads the PDF from a URL and extracts full text using pdfplumber.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(pdf_url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=response.status, detail="Failed to download PDF.")
                content = await response.read()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
            tmp_file.write(content)
            tmp_file_path = tmp_file.name

        full_text = ""
        with pdfplumber.open(tmp_file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

        return full_text.strip()
    except Exception as e:
        raise RuntimeError(f"Failed to extract PDF: {e}")

def insert_into_db(prog_number: str, text: str):
    """
    Inserts the program number, PDF text, and current date (dd-mm-yy) into PostgreSQL.
    """
    insert_query = """
    INSERT INTO guidelines (prog_number, text, date)
    VALUES (%s, %s, %s);
    """
    formatted_date = datetime.now().strftime("%d-%m-%y")
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, (prog_number, text, formatted_date))
                conn.commit()
    except Exception as e:
        raise RuntimeError(f"Database insertion failed: {e}")

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.post("/upload-pdf/")
async def upload_pdf(pdf_url: str = Form(...), prog_number: str = Form(...)):
    """
    Async endpoint to handle PDF upload from URL and store extracted content in the DB.
    """
    try:
        text = await extract_pdf_text_from_url_async(pdf_url)
        # insert_into_db is CPU-bound (blocking), so run it in threadpool
        await asyncio.to_thread(insert_into_db, prog_number, text)
        return {"message": "✅ Document loaded and stored successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
