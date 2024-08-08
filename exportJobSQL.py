from decouple import config
import time

from AnalyticsClient import AnalyticsClient

ZA_CLIENT_ID = config("ZA_CLIENT_ID")
ZA_CLIENT_SECRET = config("ZA_CLIENT_SECRET")
ZA_REFRESH_TOKEN = config("ZA_REFRESH_TOKEN")

ORGID = config("ORG_ID")
WORKSPACEID = config("WORKSPACE_ID")
VIEWID = config("VIEW_ID")

JOBID = "103074000019983004"
response_format = 'CSV'
file_path = './mmt_recon_data.csv'

ac = AnalyticsClient(
    client_id=ZA_CLIENT_ID,
    client_secret=ZA_CLIENT_SECRET,
    refresh_token=ZA_REFRESH_TOKEN
)


def zoho_get_sql(bulk, sql_query, file_path):
    JOBID = bulk.initiate_bulk_export_using_sql(sql_query, response_format)
    while True:
        response = bulk.get_export_job_details(JOBID)
        status = response
        print(status)
        if status['jobStatus'] == 'JOB COMPLETED':
            print('Export job completed successfully.')
            break
        elif status['jobStatus'] == 'ERROR OCCURRED':
            print('Export job failed.')
            break
        else:
            print('Export job is still in progress...', time.time())
            time.sleep(60)

    result = bulk.export_bulk_data(JOBID, file_path=file_path)

    print("CSV downloaded at ", time.time())


def main(retry_attempts=0):
    try:
        print("Retry Attempts ", retry_attempts)
        bulk = ac.get_bulk_instance(ORGID, WORKSPACEID)

        # # Get job details
        sql_query = """select * from `mmt-recon`"""
        zoho_get_sql(bulk, sql_query, file_path)
    except Exception as e:
        print("Exception happened in the exporting: " + str(e))
        if retry_attempts > 4:
            print("Exception occurred ")
            raise Exception(e)
        main(retry_attempts + 1)


if __name__ == "__main__":
    main()
