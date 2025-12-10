import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

def create_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not key or not url:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")
    return create_client(url, key)

def load_to_supabase(staged_path: str, table_name: str = "telco_churn"):

    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))

    print(f"🔍 Looking for data file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"❌ File not found: {staged_path}")
        return

    try:
        sb = create_supabase_client()
        df = pd.read_csv(staged_path)
        df.columns = (
            df.columns
              .astype(str)
              .str.strip()
              .str.replace(r'\s+', '_', regex=True)    # collapse any whitespace -> underscore
              .str.replace('-', '_')
              .str.replace(r'[^\w]', '', regex=True)   # remove weird punctuation (keeps letters/digits/_)
              .str.lower()
        )

        # --- define EXACT columns present in your Supabase table (lowercase) ---
        allowed_columns = [
            "tenure",
            "monthlycharges",
            "totalcharges",
            "churn",
            "internetservice",
            "contract",
            "paymentmethod",
            "tenure_group",
            "monthly_charge_segment",
            "has_internet_service",
            "is_multi_line_user",
            "contract_type_code"
        ]

        # optional: keep id out (server will populate bigserial)
        # ensure these columns exist in df; if not, create them with None so payload keys match DB
        for col in allowed_columns:
            if col not in df.columns:
                df[col] = None

        # now subset to allowed columns only, in that order
        df = df[allowed_columns]

        print("🧩 Columns being inserted (normalized & subset):", df.columns.tolist())
        batch = 200
        total_len = len(df)
        print(f"📤 Uploading {total_len} rows → {table_name}")

        for i in range(0, total_len, batch):
            chunk = df.iloc[i:i + batch].copy()
            chunk = chunk.where(pd.notnull(chunk), None)
            records = chunk.to_dict("records")

            try:
                response = sb.table(table_name).insert(records).execute()

                if hasattr(response, "error") and response.error:
                    print(f"⚠️ Batch {i//batch + 1} error: {response.error}")
                else:
                    print(f"✅ Inserted rows {i+1}–{min(i+batch, total_len)}")

            except Exception as e:
                print(f"❌ Insertion error batch {i//batch + 1}: {e}")

        print("🎯 Data loading completed.")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "churn_staged.csv")
    load_to_supabase(staged_csv_path)
