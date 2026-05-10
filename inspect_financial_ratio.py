import requests
import json
import os

BASE_URL = os.getenv("EQUIFIZ_API_BASE_URL", "https://equifizapis.cmots.com/api")
TOKEN    = os.getenv("EQUIFIZ_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImVxdWlmaXphcGlzIiwicm9sZSI6IkFkbWluIiwibmJmIjoxNzc2OTMyODcyLCJleHAiOjE4MDkxNjAwNzIsImlhdCI6MTc3NjkzMjg3MiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1MDE5MSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAxOTEifQ.lz6do_yCsDQTFz5E-qi4w825YvjFY7lWv_l1qWG4W9I")

CO_CODE     = 6    # change to any valid co_code
REPORT_TYPE = "s"  # "s" = standalone, "c" = consolidated

URL = f"{BASE_URL}/KeyFinancialRatios/{CO_CODE}/{REPORT_TYPE}"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}


def get_key_financial_ratios():
    try:
        response = requests.get(URL, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        with open("key_financial_ratios.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print("✅ API HIT SUCCESSFUL\n")
        print("Type of response:", type(data))

        if isinstance(data, list):
            print("Total records:", len(data))

            print("\n🔍 Sample records (first 3):")
            print("-" * 60)
            print(json.dumps(data[:3], indent=2))

            print("\n🧠 Keys in one record:")
            print("-" * 60)
            for key in data[0].keys():
                print(key)

            # Identify year columns
            year_cols = sorted(
                [k for k in data[0].keys() if k.startswith("Y") and k[1:].isdigit()],
                reverse=True
            )
            print("\n📅 Year/Period columns detected:")
            print("-" * 60)
            month_map = {"03": "Mar", "06": "Jun", "09": "Sep", "12": "Dec"}
            for yc in year_cols:
                try:
                    label = f"{month_map.get(yc[5:], yc[5:])} {yc[1:5]}"
                except Exception:
                    label = yc
                print(f"  {yc}  →  {label}")

            print("\n📊 All COLUMNNAME values (actual metric names):")
            print("-" * 60)
            for row in data:
                print(f"  RID: {str(row.get('RID', '')):>4}  |  {row.get('COLUMNNAME', '').strip()}")

        else:
            print(json.dumps(data, indent=2))

    except Exception as e:
        print("❌ Error:", e)


if __name__ == "__main__":
    get_key_financial_ratios()