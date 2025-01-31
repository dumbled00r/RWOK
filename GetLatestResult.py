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

dune = DuneClient("")

QUERY_ID = 4647469


def getHolders():
    """FUNCTION TO EXPORT HOLDERS TO CSV"""
    try:

        query = QueryBase(
            name="getHolders",
            query_id=QUERY_ID,
            params=[],
        )
        results = dune.get_latest_result(query)
        print(results)
        # results = dune.get_execution_results(job_id)
        # while results.state != ExecutionState.COMPLETED:
        #     try:
        #         results = dune.get_execution_results(job_id)
        #         sleep(1)
        #     except Exception:
        #         print("Too many requests 429, sleep for a while...")
        #         sleep(5)

        # print(result)
        df = pd.DataFrame(results.result.rows)
        df = df[df["total_holding_tokens"] >= 1]

        df.to_csv(f"Daosworld_Holders_snapshot.csv.csv", index=False)

        print("Wallet addresses exported to csv")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    getHolders()
