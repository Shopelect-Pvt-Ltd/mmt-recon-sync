from exportJobSQL import main as job_export
from importJobSQL import main as job_import
import time

def main(retry_attempts = 0):
    # Record the start time
    try:
        print("Retry Attempts ",retry_attempts)
        start_time = time.time()
        print("Sync starting at ", start_time)

        job_export()
        job_import()

        # Record the end time
        end_time = time.time()

        # Calculate the total seconds
        total_seconds = end_time - start_time

        print(f"Script executed in {total_seconds:.4f} seconds")
    except Exception as e:
        print(e)
        if retry_attempts > 4:
            print("Exception occurred ")
            raise Exception(e)
        main(retry_attempts + 1)

if __name__ == '__main__':
    main()