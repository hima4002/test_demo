# Snowflake Python Worksheet Script
# Goal: Run multiple rules in sequence (waterfall model), each rule filtering on previous remaining population

from snowflake.snowpark import Session
import pandas as pd
import matplotlib.pyplot as plt

# Connect to session (in Python worksheet this is already active)
session = Session.get_current_session()

# ------------------------------------
# CONFIGURATION
# ------------------------------------
# Base input table with full population
base_table = "base_data"  # should have at least RECORD_ID

# Define rules in order: (rule name, SQL logic placeholder)
# SQL must contain a placeholder: {{input_table}}
rule_definitions = [
    ("RULE_1", """
        SELECT RECORD_ID FROM {{input_table}}
        WHERE ROUND(EXPECTED_LINE / NULLIF(CREDIT_LINE, 1), 2) + 0.01 < 0.30
    """),
    ("RULE_2", """
        SELECT RECORD_ID FROM {{input_table}}
        WHERE UPC = 'LOWRTE' AND (5020 = 0 OR 5020 > 999999990 
               OR CURRENT_BALANCE / NULLIF(5020, 1) > 0.8)
    """),
    ("RULE_3_FICO_EXCLUSION", """
        SELECT RECORD_ID FROM {{input_table}}
        WHERE scr >= 620
    """),
    ("RULE_4_MATRIX_LOOKUP", """
        SELECT RECORD_ID FROM {{input_table}} -- dummy pass-through rule
    """),
    ("RULE_5_OTHER_RULE", """
        SELECT RECORD_ID FROM {{input_table}}
        WHERE SOME_FLAG = 'Y'
    """)
    # Add more rules as needed
]

# ------------------------------------
# HELPER FUNCTION
# ------------------------------------
def apply_rule(session, input_df, rule_sql, input_temp_table):
    # Register input_df as temporary view
    session.create_dataframe(input_df).create_or_replace_temp_view(input_temp_table)

    # Replace placeholder and run query
    sql = rule_sql.replace("{{input_table}}", input_temp_table)
    output_df = session.sql(sql).to_pandas()
    return output_df

# ------------------------------------
# WATERFALL EXECUTION
# ------------------------------------
# Initial load
remaining_df = session.table(base_table).select("RECORD_ID").to_pandas()
total_population = remaining_df.shape[0]

# Store results
waterfall_data = []
dropped_members_list = []  # to collect all dropped records per rule

for idx, (rule_name, rule_sql) in enumerate(rule_definitions):
    before_count = remaining_df.shape[0]
    before_rule_df = remaining_df.copy()

    # Apply rule
    remaining_df = apply_rule(session, before_rule_df, rule_sql, f"temp_input_{idx+1}")
    after_count = remaining_df.shape[0]
    dropped = before_count - after_count

    # Identify dropped members
    dropped_df = pd.merge(before_rule_df, remaining_df, on="RECORD_ID", how="left", indicator=True)
    dropped_only_df = dropped_df[dropped_df["_merge"] == "left_only"][["RECORD_ID"]]
    dropped_only_df["Dropped_At_Rule"] = rule_name
    dropped_members_list.append(dropped_only_df)

    # Record stats
    waterfall_data.append({
        "Rule": rule_name,
        "Total_Population": total_population,
        "Total_Before_This_Rule": before_count,
        "Dropped_By_This_Rule": dropped,
        "Not_Suppressed_By_This_Rule": after_count
    })

# Convert to DataFrame
wf_df = pd.DataFrame(waterfall_data)
df_dropped_all = pd.concat(dropped_members_list, ignore_index=True)

# ------------------------------------
# VISUALIZATION
# ------------------------------------
bars = [-x for x in wf_df["Dropped_By_This_Rule"]]
base = wf_df["Total_Before_This_Rule"]

plt.figure(figsize=(12, 6))
plt.bar(wf_df["Rule"], bars, bottom=base, color='salmon', label='Dropped')
plt.plot(wf_df["Rule"], wf_df["Not_Suppressed_By_This_Rule"], marker='o', color='black', label='Remaining')
plt.title("Sequential Rule Execution Waterfall")
plt.xlabel("Rule")
plt.ylabel("Record Count")
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.legend()
plt.tight_layout()
plt.show()
