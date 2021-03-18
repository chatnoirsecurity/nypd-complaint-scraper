#! env python

from shakedown import extract, fetch
from pprint import pp
from time import sleep
import csv
import json

with open('test.csv', 'w') as csvfh:

    writer = csv.writer(csvfh)

    import pdb; pdb.set_trace()

    restart_token = None
    for i in range(0,5):
        j = fetch.fetch_complaints(limit=500, restart_token=restart_token)

        print("writing raw to dump{}".format(i))
        with open("dump{}".format(i), "w") as dfh:
            json.dump(j, dfh)

        restart_token = extract.extract_restart_token(j)
        if not restart_token:
            raise SystemExit("something went wrong, I didn't get a restart token back")
        print(restart_token)

        rows = extract.extract(j)

        writer.writerows(rows)
    
        #print("sleeping 5....")
        #sleep(5)
