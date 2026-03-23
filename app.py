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
        """Your custom tag generator integrated directly."""
        formatted_loc = "".join(x for x in location.title() if x.isalnum())

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

        location_tags = [
            f"#JobsIn{formatted_loc}",
            f"#{formatted_loc}Logistics",
            f"#{formatted_loc}Jobs",
            f"#{formatted_loc}DispatchRiders",
            f"#{formatted_loc}Delivery",
            f"#{formatted_loc}Dispatch",
            f"#LastMile{formatted_loc}",
            f"Urgent dispatch rider needed in {location.title()}"
        ]

        return base_tags + location_tags

    def stream_search(self, city, session_id):
        all_leads = []
        # Get your expanded tag list
        dispatch_tags = self.get_dispatch_tags(city)
        total_tags = len(dispatch_tags)

        with DDGS() as ddgs:
            # We use a step to avoid searching 70+ tags (which might get your IP flagged)
            # Searching every 3rd tag gives a broad sweep quickly.
            # Change 'step=1' if you want to search absolutely everything.
            for i in range(0, total_tags, 2):
                tag = dispatch_tags[i]
                progress = int(((i + 1) / total_tags) * 100)

                yield f"data: {json.dumps({'progress': progress, 'status': f'Scanning Tag: {tag}'})}\n\n"

                # Combine city + specific tag for the query
                query = f'"{city}" "{tag}" "contact" OR "080"'

                try:
                    results = ddgs.text(query, max_results=8)
                    for entry in results:
                        text = f"{entry['title']} {entry['body']}"
                        emails = re.findall(self.email_pattern, text)
                        phones = re.findall(self.phone_pattern, text)
                        contacts = list(set(emails + phones))

                        for c in contacts:
                            all_leads.append({
                                'Company/Source': entry['title'][:60],
                                'Contact Detail': c,
                                'Tag Matched': tag,
                                'Link': entry['href']
                            })
                    # Keep the scrape "Clean" and human-like
                    time.sleep(random.uniform(0.3, 0.7))
                except:
                    continue

        df = pd.DataFrame(all_leads).drop_duplicates(subset=['Contact Detail'])
        results_storage[session_id] = df
        yield f"data: {json.dumps({'progress': 100, 'status': 'Leads secured!', 'done': True})}\n\n"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/progress")
def progress():
    city = request.args.get('city')
    session_id = request.args.get('session_id')
    return Response(Scraper().stream_search(city, session_id), mimetype='text/event-stream')


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
    return "File Generation Error", 404


if __name__ == "__main__":
    app.run(debug=True)