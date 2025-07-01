import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, substr
import pandas as pd

# -------------------------------
# Helper: Run Waterfall Rules
# -------------------------------
def run_rules_waterfall(session, initial_df, rules):
    waterfall_data = []
    current_df = initial_df
    dropped_df_all = session.create_dataframe([], schema=current_df.schema)

    for rule in rules:
        rule_name = rule["rule_name"]
        sql_template = rule["sql"]

        # Register current as temp view
        current_df.create_or_replace_temp_view("current_input")

        # Replace placeholder
        sql = sql_template.replace("{{input_table}}", "current_input")
        next_df = session.sql(sql)

        # Track counts
        total_before = current_df.count()
        total_after = next_df.count()
        dropped_count = total_before - total_after

        # Track dropped
        dropped_df = current_df.subtract(next_df)
        dropped_df_all = dropped_df_all.union_all(dropped_df)

        waterfall_data.append({
            "Rule": rule_name,
            "Total Population Before Rule": total_before,
            "Dropped by Rule": dropped_count,
            "Remaining After Rule": total_after
        })

        current_df = next_df

    df_waterfall = session.create_dataframe(pd.DataFrame(waterfall_data))
    return df_waterfall, dropped_df_all

# -------------------------------
# Main
# -------------------------------
def main(session: snowpark.Session):

    # Step 1: Define Table and Filtered Population
    base_df = session.table("DB.SCHEMA.TABLEA").filter(
        (col("SOURCE_DATA:PCRANDOM") == 5) &
        (col("SOURCE_DATA:DECISION_T").cast("string") == "Holdout")
    )

    # Step 2: Project required columns
    initial_df = base_df.select(
        (substr(col("SOURCE_DATA:MEMBER_NR").cast("string"), 5, 5) + 
         substr(col("SOURCE_DATA:MEMBER_NR").cast("string"), 3, 2)).alias("MEMBER_NR"),
        col("SOURCE_DATA:ACC_NO").cast("string").alias("ACCT_NUMBER"),
        col("SOURCE_DATA:CREDIT_SCORE").cast("number").alias("CREDIT_SCORE"),
        col("SOURCE_DATA:BALANCE_VALUE").cast("number").alias("BALANCE_VALUE")
    )

    # Step 3: Rules
    rules = [
        {
            "rule_name": "Rule 1 - Credit Score >= 620",
            "sql": "SELECT * FROM {{input_table}} WHERE CREDIT_SCORE >= 620"
        },
        {
            "rule_name": "Rule 2 - Balance < 5000",
            "sql": "SELECT * FROM {{input_table}} WHERE BALANCE_VALUE < 5000"
        }
    ]

    # Step 4: Execute Waterfall
    df_waterfall, dropped_df_all = run_rules_waterfall(session, initial_df, rules)

    # Show output
    df_waterfall.show()
    dropped_df_all.limit(5).show()

    return df_waterfall
