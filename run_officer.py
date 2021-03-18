#! env python

from shakedown import extract, fetch
from pprint import pp
from time import sleep
import csv
import json

with open('officer.csv', 'w') as csvfh:

    writer = csv.writer(csvfh)

    restart_token = None
    for i in range(0,100):
        j = fetch.fetch_officer_data(limit=10000, restart_token=restart_token)

        print("writing raw to dump{}".format(i))
        with open("scratch/o{}".format(i), "w") as dfh:
            json.dump(j, dfh)

        import pdb; pdb.set_trace()

        rows = extract.extract(j)

        writer.writerows(rows)

        restart_token = extract.extract_restart_token(j)
        if not restart_token:
            raise SystemExit("No more results in this dataset")
        print(restart_token)
    
        #print("sleeping 5....")
        #sleep(5)
