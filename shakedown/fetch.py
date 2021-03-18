import requests
import json
from pprint import pp

QUERY_URL = 'https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true'

HEADERS = {
    'Connection': 'keep-alive',
    'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'ActivityId': '5048db79-1b5d-376a-2653-8e1fb4c7efb4',
    'Accept': 'application/json, text/plain, */*',
    'RequestId': '22afa0e8-9496-4d9a-2e8c-4a2b50809670',
    'X-PowerBI-ResourceKey': 'b2c8d2f2-3ad1-48dc-883c-d4163a6e2d8f',
    'Content-Type': 'application/json;charset=UTF-8',
    'Origin': 'https://app.powerbigov.us',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://app.powerbigov.us/',
    'Accept-Language': 'en-US,en;q=0.9',
}

# jank jank jank jank jank jank jank jank
OFFICER_INFO_QUERY = {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"q1","Entity":"CCRB Active - Oracle","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"q1"}},"Property":"Unique Id"},"Name":"Query1.Unique Id"},{"Column":{"Expression":{"SourceRef":{"Source":"q1"}},"Property":"Command"},"Name":"Query1.Command1"},{"Column":{"Expression":{"SourceRef":{"Source":"q1"}},"Property":"Last Name"},"Name":"Query1.Last Name1"},{"Column":{"Expression":{"SourceRef":{"Source":"q1"}},"Property":"First Name"},"Name":"Query1.First Name1"},{"Column":{"Expression":{"SourceRef":{"Source":"q1"}},"Property":"Rank"},"Name":"Query1.Rank1"},{"Column":{"Expression":{"SourceRef":{"Source":"q1"}},"Property":"Shield No"},"Name":"Query1.ShieldNo"}],"OrderBy":[{"Direction":1,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"q1"}},"Property":"Command"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5]}]},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1}}}]},"QueryId":"","ApplicationContext":{"DatasetId":"523ab509-8e2d-43ed-bfad-11fcd05180d7","Sources":[{"ReportId":"f508555a-b39d-4c10-8d46-a14bc282e079"}]}}],"cancelQueries":[],"modelId":404287}

COMPLAINT_INFO_QUERY = { "version": "1.0.0", "queries": [ { "Query": { "Commands": [ { "SemanticQueryDataShapeCommand": { "Query": { "Version": 2, "From": [ { "Name": "q1", "Entity": "CCRB Active - Oracle", "Type": 0 } ], "Select": [ { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "Unique Id" }, "Name": "Query1.Unique Id" }, { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "Complaint ID" }, "Name": "CountNonNull(Query1.Complaint Id)1" }, { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "Incident Date" }, "Name": "Query1.Incident Date" }, { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "FADO Type" }, "Name": "Query1.FADO Type1" }, { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "Allegation" }, "Name": "Query1.Allegation1" }, { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "Board Disposition" }, "Name": "Query1.Board Disposition1" }, { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "NYPD Disposition" }, "Name": "Query1.NYPD Disposition" }, { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "Penalty" }, "Name": "Query1.PenaltyDesc1" } ], "Where": [ { "Condition": { "Not": { "Expression": { "Comparison": { "ComparisonKind": 0, "Left": { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "Rn" } }, "Right": { "Literal": { "Value": "0L" } } } } } } } ], "OrderBy": [ { "Direction": 1, "Expression": { "Column": { "Expression": { "SourceRef": { "Source": "q1" } }, "Property": "Complaint ID" } } } ] }, "Binding": { "Primary": { "Groupings": [ { "Projections": [ 0, 1, 2, 3, 4, 5, 6, 7 ] } ] }, "DataReduction": { "DataVolume": 3, "Primary": { "Window": { "Count": 50 } } }, "Version": 1 } } } ] }, "QueryId": "", "ApplicationContext": { "DatasetId": "523ab509-8e2d-43ed-bfad-11fcd05180d7", "Sources": [ { "ReportId": "f508555a-b39d-4c10-8d46-a14bc282e079" } ] } } ], "cancelQueries": [], "modelId": 404287 }

def _get_window_options(query):
    return query["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]["DataReduction"]["Primary"]["Window"]

def _change_query_limit(query, limit):
    if limit is not None:
        w = _get_window_options(query)
        w["Count"] = limit
    return query

def _add_restart_token(query, token_array):
    if token_array:
        w = _get_window_options(query)
        w["RestartTokens"] = token_array
    return query

def _fetch_report(query, limit=None, restart_token=None):
    _change_query_limit(query, limit)
    _add_restart_token(query, restart_token)

    r = requests.post(QUERY_URL, headers=HEADERS, data=json.dumps(query))
    return r.json()

def fetch_officer_data(limit=None, restart_token=None):
    return _fetch_report(OFFICER_INFO_QUERY, limit, restart_token)

def fetch_complaints(limit=None, restart_token=None):
    return _fetch_report(COMPLAINT_INFO_QUERY, limit, restart_token)

