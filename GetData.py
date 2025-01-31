import asyncio
import csv

from curl_cffi.requests import AsyncSession

# Session configuration
session = AsyncSession(
    impersonate="chrome120",
    headers={
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://dune.com",
        "referer": "https://dune.com/",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    },
)

# API endpoint
API_URL = "https://core-api.dune.com/public/execution"


# Fetch data from the API
async def fetch_data(offset: int, limit: int = 1000):
    response = await session.post(
        API_URL,
        json={
            "execution_id": "01JJXTTT5T6AYWDV67ZCBRWENE",
            "query_id": 4647469,
            "parameters": [],
            "pagination": {"limit": limit, "offset": offset},
        },
    )
    return response.json().get("execution_succeeded", {}).get("data", [])


# Main function to iterate through pages
async def collect_data():
    all_data = []
    limit = 1000
    total_records = 70000

    for offset in range(0, total_records, limit):
        print(f"Fetching data with offset {offset}...")
        batch = await fetch_data(offset, limit)
        if not batch:  # Stop if no data is returned
            break
        all_data.extend(batch)

    return all_data


# Save to CSV
def save_to_csv(data, filename="output.csv"):
    if not data:
        print("No data to save!")
        return

    # Extract field names dynamically
    fieldnames = data[0].keys()

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data successfully saved to {filename}")


# Run the script
async def main():
    data = await collect_data()
    save_to_csv(data)


if __name__ == "__main__":
    asyncio.run(main())
