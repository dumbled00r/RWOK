import os
from time import sleep

import pandas as pd
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.models import ExecutionState
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

load_dotenv()
DUNE_API_KEY = os.environ["DUNE_KEY"]

dune = DuneClient(api_key=DUNE_API_KEY)

QUERY_ID = 4589980


def getHolders():
    """FUNCTION TO EXPORT HOLDERS TO CSV"""
    try:

        query = QueryBase(
            name="getHolders",
            query_id=QUERY_ID,
            params=[],
        )
        job_id = dune.execute_query(query, performance="").execution_id

        results = dune.get_execution_results(job_id)
        while results.state != ExecutionState.COMPLETED:
            try:
                results = dune.get_execution_results(job_id)
                sleep(1)
            except Exception:
                print("Too many requests 429, sleep for a while...")
                sleep(5)

        # print(result)
        df = pd.DataFrame(results.result.rows)

        df.to_csv(f"Vested_HARD_Presalers_snapshot.csv", index=False)

        print("Wallet addresses exported to csv")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    getHolders()
