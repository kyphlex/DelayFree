import re
import time
import json
import pandas as pd
import io
import random
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
from ddgs import DDGS

app = FastAPI(title="Naija Logistics Scraper")

# FastAPI uses Jinja2 natively, just point it to your templates folder
templates = Jinja2Templates(directory="templates")

# Global temporary storage for results
results_storage = {}


class Scraper:
    def __init__(self):
        self.email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        self.phone_pattern = r'(\+234\d{10}|0[789][01]\d{8})'

    def get_dispatch_tags(self, location):
        base_tags = [
            "hiring dispatch rider", "dispatch rider needed", "vacancy for dispatch rider",
            "urgent dispatch rider needed", "logistics company hiring", "bike rider wanted",
            "delivery guy needed", "fleet recruitment", "riders wanted",
            "dispatch rider job vacancy", "we are hiring dispatch", "dispatch recruitment",
            "dispatch rider available", "looking for dispatch rider job", "freelance dispatch rider",
            "dispatch rider for hire", "need a dispatch job", "experienced dispatch rider",
            "licensed dispatch rider", "guarantor available dispatch", "own my bike dispatch",
            "ready to work dispatch", "errands and logistics", "waybill delivery service",
            "pickup and dropoff", "keke delivery", "food delivery rider",
            "pharmacy delivery rider", "dispatch bike remittance", "ecommerce logistics rider"
        ]
        return [f"{tag} {location}" for tag in base_tags]

    def stream_search(self, city: str, session_id: str):
        all_leads = []
        dispatch_tags = self.get_dispatch_tags(city)
        total_tags = len(dispatch_tags)

        with DDGS() as ddgs:
            for i in range(total_tags):
                tag = dispatch_tags[i]
                progress = int(((i + 1) / total_tags) * 100)

                # FastAPI StreamingResponse expects a generator yielding strings/bytes
                yield f"data: {json.dumps({'progress': progress, 'status': f'Scanning: {tag}'})}\n\n"

                query = f'{tag} "contact" OR "080" OR "email"'

                try:
                    results = ddgs.text(query, max_results=10)
                    for entry in results:
                        text = f"{entry['title']} {entry['body']}"
                        emails = re.findall(self.email_pattern, text)
                        phones = re.findall(self.phone_pattern, text)
                        contacts = list(set(emails + phones))

                        for c in contacts:
                            all_leads.append({
                                'Company/Source': entry['title'][:70],
                                'Contact Detail': c,
                                'Keyword Used': tag,
                                'Link': entry['href']
                            })
                    time.sleep(random.uniform(0.2, 0.5))

                except Exception as e:
                    # The Safety Net: If DDGS blocks us, save data and exit gracefully
                    error_msg = str(e).lower()
                    if "403" in error_msg or "rate limit" in error_msg:
                        yield f"data: {json.dumps({'progress': 100, 'status': 'Rate limit hit! Saving what we found...', 'done': True})}\n\n"
                        break
                    else:
                        time.sleep(2)
                        continue

        df = pd.DataFrame(all_leads).drop_duplicates(subset=['Contact Detail'])
        results_storage[session_id] = df
        yield f"data: {json.dumps({'progress': 100, 'status': 'All tags scanned! Ready.', 'done': True})}\n\n"


# --- FASTAPI ROUTES ---

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"request": request}
    )

@app.get("/progress")
async def progress(city: str, session_id: str):
    # Native, headache-free streaming
    scraper = Scraper()
    return StreamingResponse(scraper.stream_search(city, session_id), media_type='text/event-stream')


@app.get("/download")
async def download(city: str, session_id: str):
    df = results_storage.get(session_id)

    if df is not None and not df.empty:
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)

        # FastAPI way to force a file download
        headers = {'Content-Disposition': f'attachment; filename="dispatch_leads_{city}.csv"'}
        return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers=headers)

    return {"error": "No leads found or session expired. Try again."}