#! env python
# for when you downloaded but it didn't extract into csv

from shakedown import extract, fetch
from pprint import pp
from time import sleep
import csv
import json

with open('manual.csv', 'w') as csvfh:

    writer = csv.writer(csvfh)

    with open("scratch/dump10") as fh:
        j = json.load(fh)

        rows = extract.extract(j)

        writer.writerows(rows)
