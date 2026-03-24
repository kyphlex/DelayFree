import re
import time
import json
import pandas as pd
import io
import random
from flask import Flask, render_template, request, send_file, Response
from ddgs import DDGS

app = Flask(__name__)
app.secret_key = "logistics_power_scraper_2026"

results_storage = {}


class Scraper:
    def __init__(self):
        self.email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        self.phone_pattern = r'(\+234\d{10}|0[789][01]\d{8})'

    def get_dispatch_tags(self, location):
        # Your base tags exactly as provided
        base_tags = [
            "#HiringDispatchRiders", "#DispatchRiderJobs", "#LogisticsJobs",
            "#NowHiringRiders", "#DeliveryJobs", "#DispatchJobs", "#WeAreHiring",
            "#FleetRecruitment", "#LookingForDeliveryJobs", "#DispatchRiderForHire",
            "#FreelanceRider", "#FullTimeRider", "#ReadyToRide", "#DeliveryDriver",
            "#HireARider", "#HyperlocalDelivery", "#QuickCommerce", "#QCommerce",
            "#LastMileDelivery", "#SameDayDelivery", "#InstantDelivery",
            "#NeighborhoodDelivery", "#OnDemandDelivery", "#FoodDeliveryRider",
            "#GroceryDelivery", "#PharmacyDelivery", "#ErandsAndLogistics",
            "#B2BDelivery", "#EcommerceLogistics", "#MotorcycleDispatch",
            "#BicycleCourier", "#DeliveryVan", "#TricycleDelivery", "#EvDelivery",
            "#CargoBike", "#DeliveryBox", "#DispatchBike", "#RiderGear",
            "#CoolingBoxDelivery", "#ExperiencedRider", "#SafeRider", "#DefensiveDriving",
            "#RouteOptimization", "#GPSNavigation", "#PunctualDelivery",
            "#CustomerServiceSkills", "#ValidRidersCard", "#LicensedRider",
            "#CleanDrivingRecord", "#GuarantorAvailable", "#DispatchLife",
            "#LifeOfARider", "#LogisticsCompany", "#DeliveryService", "#HustleAndMotivate",
            "#DailyGrind", "#BehindTheHandlebars", "#SendIt", "#WeDeliver",
            "#FastAndReliable", "#PackageSecured",
            "How to apply for dispatch rider jobs",
            "Best logistics company to work for as a rider",
            "Companies hiring freelance delivery riders",
            "Same-day hyperlocal delivery services near me",
            "Average salary for a dispatch rider"
        ]

        # We attach the location to EVERY tag via f-string for absolute generic search
        return [f"{tag} in {location}" for tag in base_tags]

    def stream_search(self, city, session_id):
        all_leads = []
        dispatch_tags = self.get_dispatch_tags(city)
        total_tags = len(dispatch_tags)

        with DDGS() as ddgs:
            # step=1: We search EVERY tag. No skipping.
            for i in range(total_tags):
                tag = dispatch_tags[i]
                progress = int(((i + 1) / total_tags) * 100)

                yield f"data: {json.dumps({'progress': progress, 'status': f'Scanning: {tag}'})}\n\n"

                # Query is now simplified: it's just your location-injected tag + contact triggers
                query = f'{tag} "contact" OR "080" OR "email"'

                try:
                    # max_results=10 to keep the deep search going
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
                    # Slight delay to keep the connection healthy
                    time.sleep(random.uniform(0.2, 0.5))
                except:
                    # If we hit a rate limit, pause a bit longer
                    time.sleep(2)
                    continue

        df = pd.DataFrame(all_leads).drop_duplicates(subset=['Contact Detail'])
        results_storage[session_id] = df
        yield f"data: {json.dumps({'progress': 100, 'status': 'All tags scanned! Ready.', 'done': True})}\n\n"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/progress")
def progress():
    city = request.args.get('city')
    session_id = request.args.get('session_id')
    return Response(self_stream := Scraper().stream_search(city, session_id), mimetype='text/event-stream')


@app.route("/download")
def download():
    session_id = request.args.get('session_id')
    city = request.args.get('city')
    df = results_storage.get(session_id)

    if df is not None:
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(buf, mimetype="text/csv", as_attachment=True, download_name=f"dispatch_leads_{city}.csv")
    return "Error: Data expired or not found", 404


if __name__ == "__main__":
    app.run(debug=True)