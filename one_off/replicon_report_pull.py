#%%
import csv
import requests
import json
import datetime
import time
import re
import logging
import azure.functions as func

CREATE_REPORT_ENDPOINT = "https://na5.replicon.com/RefinedTechnologiesInc/services/ReportService1.svc/CreateReportGenerationBatch"
EXECUTE_REPORT_ENDPOINT = "https://na5.replicon.com/RefinedTechnologiesInc/services/ReportService1.svc/ExecuteBatchInBackground"
STATUS_REPORT_ENDPOINT = "https://na5.replicon.com/RefinedTechnologiesInc/services/ReportService1.svc/GetBatchStatus"
RESULTS_REPORT_ENDPOINT = "https://na5.replicon.com/RefinedTechnologiesInc/services/ReportService1.svc/GetReportGenerationBatchResults"
REPORT_URI = "urn:replicon-tenant:8b5f0b5d16f248ef9a1f7debdc7a6043:report:182b03da-e8f4-4b92-912a-d36ec4249556"
FORMAT_URI = "urn:replicon:report-output-format-option:csv"
SUCCEEDED = "urn:replicon:batch-execution-state:succeeded"
FAILED = "urn:replicon:batch-execution-state:failed"
RESTART = "urn:replicon:batch-execution-state:restart"
IN_PROGRESS = "urn:replicon:batch-execution-state:in-progress"
UUID_REGEX = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
token = 

def query_api(endpoint, body, headers):
    try:
        response = requests.post(endpoint, json=body, headers=headers, timeout=600)
        payload = json.loads(response.text)["d"]
    except Exception as e:
        raise e
    return payload


def create_report(headers):
    body = {
        "reportParameters": [
            {
                "reportUri": f"{REPORT_URI}",
                "filterValues": [],
                "outputFormatUri": f"{FORMAT_URI}"
            }
        ]
    }
    batch_uri = query_api(CREATE_REPORT_ENDPOINT, body, headers)
    logging.info("creating batch")
    return batch_uri


def execute_report(headers, batch_uri):
    body = {"batchUri": batch_uri}
    query_api(EXECUTE_REPORT_ENDPOINT, body, headers)
    logging.info("poking batch")


def wait_for_report(headers, batch_uri):
    body = {"batchUri": batch_uri}
    payload = query_api(STATUS_REPORT_ENDPOINT, body, headers)
    status = payload.get("executionState")
    logging.info("batch in progress")
    return status


def get_report(headers, batch_uri):
    body = {"reportGenerationBatchUri": batch_uri}
    payload = query_api(RESULTS_REPORT_ENDPOINT, body, headers)
    report = payload["reportGenerationResults"][0]["payload"]
    logging.info("retrieving batch")
    return report


def process_transaction(transaction):
    date_fields = ["entry_date", "submitted_date", "timesheet_start_date", "timesheet_end_date"]
    boolean_fields = ["is_fatigue_day_earned", "is_travel_night_away", "is_stipend_eligible"]
    for key, value in transaction.items():

        # Convert Date Values
        if key in date_fields:
            try:
                updated_value = datetime.datetime.strptime(value, "%b %d, %Y").strftime("%Y-%m-%d")
                transaction[key] = updated_value
            except ValueError:
                pass

        # Convert Boolean Values
        if key in boolean_fields and value == "Yes":
            transaction[key] = True
        elif key in boolean_fields and value == "":
            transaction[key] = False

    return transaction


def parse_report(state, report):
    transactions = []
    reader = csv.reader(report.splitlines(), delimiter=',', quotechar='"')
    for line in reader:
        transaction = {
            "user_name": f"{line[0]}",
            "employee_id": f"{line[1]}",
            "user_department_name": f"{line[2]}",
            "entry_date": f"{line[3]}",
            "entry_id": f"{line[4]}",
            "approval_status": f"{line[5]}",
            "submitted_date": f"{line[6]}",
            "project_code": f"{line[7]}",
            "activity_name": f"{line[8]}",
            "task_name": f"{line[9]}",
            "hours_worked": f"{line[10]}",
            "is_stipend_eligible": f"{line[12]}",
            "is_travel_night_away": f"{line[13]}",
            "is_fatigue_day_earned": f"{line[14]}",
            "timesheet_period": f"{line[15]}",
            "project_name": f"{line[16]}",
            "activity_code": f"{line[17]}",
            "time_in": f"{line[18]}",
            "time_out": f"{line[19]}",
            "timesheet_start_date": f"{line[20]}",
            "timesheet_end_date": f"{line[21]}"
        }
        transaction = process_transaction(transaction)
        if not filter_transaction(state, transaction): transactions.append(transaction)
        
    return transactions


def filter_transaction(state, t):
    if re.match(UUID_REGEX, t.get("entry_id")) and t.get("submitted_date") != "" and t.get("time_in") == "" and t.get("time_out") == "":
        try:
            if state == {}: return False
            elif state.get("cursor"):
                cursor_date = datetime.datetime.strptime(state["cursor"], "%Y-%m-%d").date()
                submitted_on = datetime.datetime.strptime(t.get("submitted_date"), "%Y-%m-%d").date()
                delta = cursor_date - submitted_on
                if submitted_on >= cursor_date: return False
                if delta.days <= 2: return False
        except ValueError:
            pass
    return True


def finalize_payload(transactions):
    payload = {
      "state": {"cursor": datetime.datetime.now().strftime("%Y-%m-%d")},
      "insert": {"time_entry": transactions},
      "schema": {"time_entry": {"primary_key": ["entry_id"]}},
      "hasMore": "false"
    }
    return payload


def main(req: func.HttpRequest) -> func.HttpResponse:
    request = req.get_json()
    try:
        token = request.get("secrets")["apiToken"]
        state = request.get("state")
        headers = {"Authorization": f"Bearer {token}"}
    except ValueError as e:
        logging.info(e)
        raise e

    batch_uri = create_report(headers)
    execute_report(headers, batch_uri)

    while True:
        status = wait_for_report(headers, batch_uri)
        if status == SUCCEEDED: break
        elif status == FAILED: break
        elif status == RESTART: break
        elif status == IN_PROGRESS: time.sleep(3)
        elif status is None: break

    report = get_report(headers, batch_uri)
    transactions = parse_report(state, report)
    payload = finalize_payload(transactions)

    return func.HttpResponse(body=json.dumps(payload), status_code=200, mimetype="application/json")



# %%
