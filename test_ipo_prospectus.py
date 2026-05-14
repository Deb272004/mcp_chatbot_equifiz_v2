# import requests
# import json

# URL = "https://equifizapis.cmots.com/api/IPOProspectus/sebi"

# # ── Replace with your actual token ──────────────────────────────────────────
# TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImVxdWlmaXphcGlzIiwicm9sZSI6IkFkbWluIiwibmJmIjoxNzc2OTMyODcyLCJleHAiOjE4MDkxNjAwNzIsImlhdCI6MTc3NjkzMjg3MiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1MDE5MSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAxOTEifQ.lz6do_yCsDQTFz5E-qi4w825YvjFY7lWv_l1qWG4W9I"

# headers = {
#     "Authorization": f"Bearer {TOKEN}",
#     "Content-Type": "application/json",
# }

# response = requests.get(URL, headers=headers)

# print(f"Status Code : {response.status_code}")
# print(f"URL         : {URL}")
# print("-" * 60)

# try:
#     data = response.json()

#     # Print raw structure first
#     print("Raw Response (first 2 records):")
#     print(json.dumps(data[:2] if isinstance(data, list) else data, indent=2))

#     print("-" * 60)

#     # If it's a list, inspect keys from first record
#     if isinstance(data, list) and len(data) > 0:
#         print(f"Total records : {len(data)}")
#         print(f"Keys in record: {list(data[0].keys())}")
#         print()
#         print("Sample record:")
#         for k, v in data[0].items():
#             print(f"  {k:<20} : {v}")

#     # If it's a dict, look for a nested rows/data key
#     elif isinstance(data, dict):
#         print("Top-level keys:", list(data.keys()))
#         # Try common wrapper patterns
#         for key in ("data", "Data", "rows", "result", "Result", "Table"):
#             if key in data:
#                 rows = data[key]
#                 print(f"\nFound rows under '{key}' — {len(rows)} records")
#                 if rows:
#                     print(f"Keys: {list(rows[0].keys())}")
#                     print()
#                     print("Sample record:")
#                     for k, v in rows[0].items():
#                         print(f"  {k:<20} : {v}")
#                 break

# except Exception as e:
#     print(f"Error parsing response: {e}")
#     print("Raw text:", response.text[:500])


import requests
import json

# Configuration
BASE_URL = "https://equifizapis.cmots.com/api"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImVxdWlmaXphcGlzIiwicm9sZSI6IkFkbWluIiwibmJmIjoxNzc2OTMyODcyLCJleHAiOjE4MDkxNjAwNzIsImlhdCI6MTc3NjkzMjg3MiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1MDE5MSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAxOTEifQ.lz6do_yCsDQTFz5E-qi4w825YvjFY7lWv_l1qWG4W9I"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

def get_profit_and_loss_results():
    """
    Fetches the specific Quarterly Results (P&L) for a company.
    Endpoint: /api/BalanceSheet/{co_code}/{type}
    """
    # Note: report_type is 'S' for Standalone, 'C' for Consolidated
    # https://equifizapis.cmots.com/api/ETFGetQuotes/NSE/INF789F1AUX7
    # https://equifizapis.cmots.com/api/ETFShareholdingEquity/INF373I01023
    url = "https://equifizapis.cmots.com/api/DailyRatios/476/C"
    
    print(f"Requesting: {url}\n")
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        print(data)

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    # 476 is the code visible in your mapping image
    get_profit_and_loss_results()