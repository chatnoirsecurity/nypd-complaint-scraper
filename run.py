#! env python
from shakedown import extract, fetch
from time import sleep
import argparse
import csv
from datetime import date
from collections import namedtuple

Report = namedtuple("Report", ["func", "filename", "cols"])

# TODO - just pull these out of the data we get back 
# 000002,001 DET,Smart,John,Police Officer,2323
OFFICER_COLS = ["id", "command", "lastname", "firstname", "rank", "badge_no"]

# 015156,200000038,2000-01-03,Abuse of Authority,Question and/or stop,Exonerated,blarg
COMPLAINT_COLS = ["officer_id", "complaint_id", "date", "fado_type", "allegation", "board_disposition", "nypd_disposition", "penalty"]

active_officer = Report(fetch.fetch_active_officer_data, "active_officers.csv", OFFICER_COLS)
inactive_officer = Report(fetch.fetch_inactive_officer_data, "inactive_officers.csv", OFFICER_COLS)
active_complaint = Report(fetch.fetch_active_complaints, "active_complaints.csv", COMPLAINT_COLS)
inactive_complaint = Report(fetch.fetch_inactive_complaints, "inactive_complaints.csv", COMPLAINT_COLS)

def run(args):

    #filename_prefix = date.today().isoformat() 

    reports_to_run = []
    if args.all:
        reports_to_run = [
            active_officer, 
            inactive_officer, 
            active_complaint, 
            inactive_complaint
        ]
    else:
        # TODO make this less jank and allow for multiple reports
        # to be run at once
        if args.report == "officer" and args.type == "active":
            reports_to_run.append(active_officer)
        if args.report == "officer" and args.type == "inactive":
            reports_to_run.append(inactive_officer)
        if args.report == "complaint" and args.type == "active":
            reports_to_run.append(active_complaint)
        if args.report == "complaint" and args.type == "inactive":
            reports_to_run.append(inactive_complaint)

    if not reports_to_run:
        raise SystemExit("nothing to do, quitting")

    for report in reports_to_run: 
        scrape_to_filename(report, limit=args.limit)
    

def scrape_to_filename(report, limit):
    report_func = report.func

    print("Fetching report, writing to {}".format(report.filename))
    with open(report.filename, 'w') as csvfh:
        writer = csv.writer(csvfh)

        writer.writerow(report.cols)

        restart_token = None
        # TODO take this out and add in a better check against looping forever
        for i in range(0,100):
            print("Fetching page {}...".format(i))

            j = report_func(limit=limit, restart_token=restart_token)
            rows = extract.extract(j)

            writer.writerows(rows)

            restart_token = extract.extract_restart_token(j)
            if restart_token:
                sleep(2)
            else:
                print("Done!")
                break

if __name__ == "__main__":
    cli = argparse.ArgumentParser(description='Scrape the NYPD complaint database')

    cli.add_argument('-r', '--report', dest="report", choices=['officer', 'complaint'], help="Which report you want to scrape (officer data or complaint data). Use all to scrape both")
    cli.add_argument('-t', '--type', dest="type", choices=['active', 'inactive'], help="What version of the report you want to scrape (active or inactive officers)")
    cli.add_argument('-A', '--all', dest="all", action="store_true", help="Shortcut to download everything")
    cli.add_argument('--chunk_size', dest="limit", default=10000, type=int, help="How many records to pull per request as we're scraping. Microsoft BI has a limit of 20K.")
    args = cli.parse_args()
        
    run(args)