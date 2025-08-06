import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import CellIsRule, FormulaRule
import os
from openpyxl.styles import Font
from openpyxl.styles import PatternFill
import pandas as pd
import random
import calendar
from datetime import datetime, date, timedelta
from dateutil.parser import parse

def load_shift_requirements(excel_file_path, sheet_name="Shifts"):
    """
    Load minimum staffing requirements from the Excel file.
    """
    try:
        shift_requirements = pd.read_excel(
            excel_file_path,
            sheet_name=sheet_name,
            usecols=["Starting hour", "Minimum required", "Social agents needed"]
        )
        shift_requirements.rename(columns={"Social agents needed": "smt_needed"},
    inplace=True)
        shift_requirements.columns = ["starting_hour", "min_required", "smt_needed"]
        shift_requirements.set_index("starting_hour", inplace=True)
        shift_requirements.index = shift_requirements.index.astype(str)
        return shift_requirements
    except Exception as e:
        print(f"Error loading shift requirements: {e}")
        return None

def import_sheet(excel_file_path, sheet_name): #function to load a sheet
    try:
        teamsheet = openpyxl.load_workbook(excel_file_path)
        sheet = teamsheet[sheet_name]
        print(f"{excel_file_path} loaded successfully!")
        return sheet
    except FileNotFoundError:
        print(f"The file {excel_file_path} was not found")
    except KeyError:
        print(f"The sheet {sheet_name} was not found")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None

def load_holiday_calendar(file_path, sheet_name):
    try:
        df_hol = pd.read_excel(file_path, sheet_name=sheet_name, index_col=0) #load the excel sheet

        return df_hol
    except Exception as e:
        print(f"Error loading holiday calendar:{e}")
        return None

def process_holiday_calendar(df_hol):
    try:
        df_long = df_hol.melt(ignore_index=False, var_name="Employee", value_name="Status")
        df_long["on_leave"] = df_long["Status"] == "OFF" # replace OFF wit True
        df_long = df_long.drop(columns=["Status"])
        df_long = df_long.reset_index().rename(columns={"index":"Date"}) #make date a normal column
        print(df_long.head())
        return df_long
    except Exception as e:
        print(f"Error processing holiday calendar:{e}")
        return None

def integrate_holidays_into_schedule(df_schedule, df_long):
    df_schedule = df_schedule.merge(
        df_long.rename(columns={"Date": "Day"}),
        on=["Day", "Employee"],
        how="left"
    )
    df_schedule.loc[df_schedule["on_leave"] == True, "shift_time"] = "AL"
    df_schedule = df_schedule.drop(columns=["on_leave"])
    return df_schedule

#creating the schedule structure to be called before integrate holiday
def create_schedule_structure(df_holidays, employee_list, start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date) #generate all dates beween the start and end dates

    df_schedule = pd.DataFrame([(day, emp) for day in date_range for emp in employee_list],columns=["Day", "Employee"]) #new table merging both date and employees
    df_schedule["shift_time"] = None #add a column to the df_schedule variable
    return df_schedule

#Loading employee list
def normalize_smt_column(series):
    return (
        series
        .apply(lambda x: str(x).strip().lower() if pd.notna(x) else "")
        .map({"yes": True, "true": True, "1": True, "y": True, "✓": True})
        .fillna(False)
    )

def load_employee_list(file_path):
    try:
        df_team = pd.read_excel(file_path, sheet_name="Team")

        # Clean column names
        df_team.columns = df_team.columns.str.strip().str.lower()
        print("🧾 Raw values from 'trained social':", df_team["trained social"].unique())
        df_team = df_team.rename(columns={
            "agent's name": "name",
            "trained social": "trained_social",
            "trained t2": "trained_t2",
            "trained c&c": "trained_cc",
            "days per week": "days_per_week",
            "hours per week": "hours_per_week",
        })

        # ─── Normalize the TRUE/FALSE strings into real booleans ───
        df_team["trained_social"] = (
            df_team["trained_social"]
              .astype(str)             # ensure strings
              .str.strip()             # trim whitespace
              .str.upper()             # "True" → "TRUE"
              .map({"TRUE": True, "FALSE": False})
              .fillna(False)           # anything else → False
        )

        print("✅ trained_social value counts:", df_team["trained_social"].value_counts())

        df_team = df_team.dropna(how="all")  # Remove empty rows if any
        return df_team
    except Exception as e:
        print(f"Error loading employee list: {e}")
        return None


def merge_df_team_and_df_schedule(df_team, df_schedule):
    df_schedule = df_schedule.merge(
        df_team[["name", "trained_social", "team"]],
        how="left",
        left_on="Employee",
        right_on="name"
    )
    return df_schedule

#extracting the social media trained people
def extract_smt_staff_grouped(file_path):
    try:
        # Load the dataset from the Excel file
        df = pd.read_excel(file_path, sheet_name="Team")
        # ✅ Clean column names
        df.columns = df.columns.str.strip().str.lower()
        df = df.rename(columns={
            "agent's name": "name",
            "trained social": "trained_social",
            "trained t2": "trained_t2",
            "team": "team"
        })
        df["trained_social"] = normalize_smt_column(df["trained_social"])

        # ✅ Filter for SMT-trained staff
        smt_team = df[(df["trained_social"] == True) & (df["trained_t2"] == False)]

        # Group by team
        grouped_smt_team = smt_team.groupby("team")["name"].apply(list).to_dict()

        return smt_team, grouped_smt_team
    except Exception as e:
        print(f"Error extracting SMT staff: {e}")
        return None, None

# Checking if at least 2 SMT staff are available per shift
def check_smt_needed(df_schedule):
    grouped = df_schedule.groupby("shift_time")["trained_social"].sum()
    return grouped >= 2  # Returns True if at least 2 SMT are scheduled

def group_employees_by_language(file_path):
    try:
        df = pd.read_excel(file_path, sheet_name="Team")
        df.columns = df.columns.str.strip().str.lower()
        df = df.rename(columns={"agent's name": "name"})
        language_groups = df.groupby("team")["name"].apply(list).to_dict()
        return language_groups
    except Exception as e:
        print(f"Error grouping employees by language: {e}")
        return None
#pick random staff to be used inside the relevant functions
def pick_random_staff(candidates, exclude=None):
    # If no candidates at all, return None
    if not candidates:
        return None
    # Exclude any names in the exclude list
    if exclude:
        candidates = [c for c in candidates if c not in exclude]
        if not candidates:
            return None
    # Pick randomly from the remaining candidates
    return random.choice(candidates)

def assign_initial_shifts(df_schedule):
    df_schedule["Weekday"] = pd.to_datetime(df_schedule["Day"]).dt.weekday
    # Assign 09:00-18:00 to everyone Monday to Saturday
    df_schedule.loc[(df_schedule["Weekday"] < 5) &
                    (df_schedule["shift_time"] != "AL"), "shift_time"] = "9"
    df_schedule.loc[(df_schedule["Weekday"] > 4) &
                    (df_schedule["shift_time"] != "AL"), "shift_time"] = "RDO"

    return df_schedule

def select_fairest(df_history, df_team, candidates, shift_type, top_n=10, required_count=None, filter_func=None, duplicate_teams=True):
    # 🔒 Make sure candidates is a list of names
    if isinstance(candidates, pd.DataFrame):
        if "name" in candidates.columns:
            candidates = candidates["name"].tolist()
        else:
            raise ValueError("DataFrame passed as 'candidates' must contain a 'name' column.")
    elif not isinstance(candidates, list):
        candidates = list(candidates)
    # start with just the names
    candidates_df = pd.DataFrame({"name": candidates})
    candidates_df = candidates_df.dropna(subset=["name"])
    candidates_df = candidates_df.merge(df_team[["name", "team", "trained_social"]], on="name", how="left")
    candidates_df["name"] = candidates_df["name"].astype(str)

    # now your filter_func can see a 'team' column
    if filter_func:
        mask = candidates_df.apply(filter_func, axis=1).astype(bool)
        candidates_df = candidates_df.loc[mask]

    merged = candidates_df.merge(df_history.drop(columns=["team"], errors="ignore"), on="name", how="left")


    # fallback if there are no candidates left after filtering
    if merged.empty:
        print("❌ No candidates available after filtering.")
        return pd.DataFrame({"name": []})

    short = (
        merged
        .sort_values([f"count_{shift_type}", f"latest_{shift_type}"])
        .head(top_n)
    )
    if not duplicate_teams:
        short = short.drop_duplicates(subset=["team"], keep="first")

    if not duplicate_teams and len(short) < required_count:
            print(f"⚠ Not enough unique teams found for {shift_type} (got {len(short)}, need {required_count})")

    if required_count is not None:
        # Keep expanding top_n until we get enough people or reach all available
        while short.shape[0] < required_count:
            if top_n >= merged.shape[0]:
                print(f"⚠ Only {short.shape[0]} candidates available, but {required_count} required.")
                break  # prevent infinite loop
            top_n += 1
            short = merged.sort_values([f"count_{shift_type}", f"latest_{shift_type}"]).head(top_n)
            print(f"🔁 {short.shape[0]} candidates selected for {shift_type}, aiming for {required_count}")

        short = short.head(required_count)

    return short.sample(frac=1).reset_index(drop=True)

def update_history(df_history, df_week,weekday_late_late=None, weekday_early_late=None, saturday_late_late=None, saturday_early_late=None, saturday_morning=None):

    if weekday_late_late:
        for name in weekday_late_late:
            df_history.loc[df_history["name"] == name, "count_late_late"] += 1
            mask = (df_week["Employee"] == name) & (df_week["shift_time"].apply(classify_shift) == "late_late")
            latest_day = df_week.loc[mask, "Day"].max()
            if pd.notna(latest_day):
                df_history.loc[df_history["name"] == name, "latest_late_late"] = latest_day.strftime("%Y-W%U")

    if weekday_early_late:
        for name in weekday_early_late:
            df_history.loc[df_history["name"] == name, "count_early_late"] += 1
            mask = (df_week["Employee"] == name) & (df_week["shift_time"].apply(classify_shift) == "early_late")
            latest_day = df_week.loc[mask, "Day"].max()
            if pd.notna(latest_day):
                df_history.loc[df_history["name"] == name, "latest_early_late"] = latest_day.strftime("%Y-W%U")

    if saturday_late_late:
        for name in saturday_late_late:
            df_history.loc[df_history["name"] == name, "count_saturday_late_late"] += 1
            mask = (
                    (df_week["Employee"] == name) &
                    (df_week["shift_time"].apply(classify_shift) == "late_late") &
                    (df_week["Day"].dt.weekday == 5)
            )

            latest_day = df_week.loc[mask, "Day"].max()

            if pd.notna(latest_day):
                df_history.loc[df_history["name"] == name, "latest_saturday_late_late"] = latest_day.strftime("%Y-W%U")

    if saturday_early_late:
        for name in saturday_early_late:
            df_history.loc[df_history["name"] == name, "count_saturday_early_late"] += 1
            mask = (
                    (df_week["Employee"] == name) &
                    (df_week["shift_time"].apply(classify_shift) == "early_late") &
                    (df_week["Day"].dt.weekday == 5)
            )

            latest_day = df_week.loc[mask, "Day"].max()

            if pd.notna(latest_day):
                df_history.loc[df_history["name"] == name, "latest_saturday_early_late"] = latest_day.strftime("%Y-W%U")

    if saturday_morning is not None and not saturday_morning.empty:
        for name in saturday_morning:
            df_history.loc[df_history["name"] == name, "count_saturday_morning"] += 1
            mask = (
                    (df_week["Employee"] == name) &
                    (df_week["shift_time"].apply(classify_shift) == "morning") &
                    (df_week["Day"].dt.weekday == 5)
            )

            latest_day = df_week.loc[mask, "Day"].max()

            if pd.notna(latest_day):
                df_history.loc[df_history["name"] == name, "latest_saturday_morning"] = latest_day.strftime("%Y-W%U")

    return df_history
# Revised assign_both_late_shifts: pre‐select SMT to cover both early_late & late_late,
# then fill any remaining slots with non‐SMT to meet overall headcount.
def assign_both_late_shifts(df_week, df_history, df_team, grouped_smt_team, shift_requirements):
    week = df_week["Day"].dt.isocalendar().week.iloc[0]
    used_languages = set()

    # SMT requirements per shift
    smt_req_13 = int(shift_requirements.loc["13", "smt_needed"])
    smt_req_15 = int(shift_requirements.loc["15", "smt_needed"])
    total_smt_req = smt_req_13 + smt_req_15

    # Build SMT pool and select SMTs
    smt_pool = df_team[df_team["trained_social"]]["name"].tolist()
    smt_selected = select_fairest(
        df_history,
        df_team,
        smt_pool,
        shift_type="late_late",
        top_n=len(smt_pool),
        required_count=total_smt_req,
        duplicate_teams=False
    )
    smt_selected = smt_selected["name"].tolist()

    # Assign SMTs to late_late then early_late
    late_late_team = smt_selected[:smt_req_15]
    early_late_team = smt_selected[smt_req_15:]

    # Record used languages
    for name in smt_selected:
        lang = df_team.loc[df_team["name"] == name, "team"].iloc[0]
        used_languages.add(lang)

    # Headcounts required
    headcount_13 = int(shift_requirements.loc["13", "min_required"])
    headcount_15 = int(shift_requirements.loc["15", "min_required"])

    # Non-SMT candidates for filling
    all_candidates = df_team["name"].tolist()
    filler_pool = [n for n in all_candidates if n not in smt_selected]

    # Fill remaining late_late slots
    remaining_15 = headcount_15 - len(late_late_team)
    if remaining_15 > 0:
        if remaining_15 > len(filler_pool):
            print(f"⚠ Not enough filler candidates for late_late (needed {remaining_15}, available {len(filler_pool)})")
            remaining_15 = len(filler_pool)
        more_15 = select_fairest(
            df_history,
            df_team,
            filler_pool,
            shift_type="late_late",
            top_n=len(filler_pool),
            required_count=remaining_15,
            filter_func=lambda r: r["team"] not in used_languages,
            duplicate_teams=False
        )
        more_15 = more_15["name"].tolist()
        late_late_team += more_15
        for name in more_15:
            used_languages.add(df_team.loc[df_team["name"] == name, "team"].iloc[0])

    # Fill remaining early_late slots
    remaining_13 = headcount_13 - len(early_late_team)
    if remaining_13 > 0:
        if remaining_13 > len(filler_pool):
            print(f"⚠ Not enough filler candidates for early_late (needed {remaining_13}, available {len(filler_pool)})")
            remaining_13 = len(filler_pool)
        more_13 = select_fairest(
            df_history,
            df_team,
            filler_pool,
            shift_type="early_late",
            top_n=len(filler_pool),
            required_count=remaining_13,
            filter_func=lambda r: r["team"] not in used_languages,
            duplicate_teams=False
        )
        more_13 = more_13["name"].tolist()
        early_late_team += more_13
        for name in more_13:
            used_languages.add(df_team.loc[df_team["name"] == name, "team"].iloc[0])

    # Assign agents into df_week
    for name in late_late_team:
        days = df_week[(df_week["Employee"] == name) & (df_week["Weekday"] < 5)]["Day"].tolist()
        for d in days:
            assign_one_agent(df_week, d, "15", name)
    df_week.loc[df_week["Employee"].isin(late_late_team), "assigned_weekly_15"] = True

    for name in early_late_team:
        days = df_week[(df_week["Employee"] == name) & (df_week["Weekday"] < 5)]["Day"].tolist()
        for d in days:
            assign_one_agent(df_week, d, "13", name)
    df_week.loc[df_week["Employee"].isin(early_late_team), "assigned_weekly_13"] = True

    return df_week, late_late_team, early_late_team

def enforce_minimum_staffing(df_schedule, shift_requirements):
    """
    Ensures at least shift_requirements.loc[shift, 'min_required'] people are assigned
    to each (Day, shift_time) in df_schedule.
    """
    # 1. Count current assignments per day & shift
    counts = (
        df_schedule
        .groupby(["Day", "shift_time"])
        .size()
        .rename("assigned_count")
        .reset_index()
    )

    # 2. Merge in the min_required for each shift_time
    counts = counts.merge(
        shift_requirements["min_required"].rename("min_required"),
        left_on="shift_time",
        right_index=True,
        how="left"
    )
    counts = counts[counts["min_required"].notna()]

    # 3. For any (Day, shift_time) where assigned_count < min_required, log or fix
    for _, row in counts.iterrows():
        day, shift_time = row["Day"], row["shift_time"]
        needed = int(row["min_required"] - row["assigned_count"])
        if needed <= 0:
            continue

        print(f"⚠ {day.date()} {shift_time}: need {needed} more staff")

        for _ in range(needed):
            mask_day = df_schedule["Day"] == day
            mask_shift = df_schedule["shift_time"].isin(["9", "RDO"])
            free_df = df_schedule[mask_day & mask_shift]

            if classify_shift(shift_time) == "early_late":
                free_df = free_df[free_df["assigned_weekly_13"] == True]
            elif classify_shift(shift_time) == "late_late":
                free_df = free_df[free_df["assigned_weekly_15"] == True]

            candidates = free_df["Employee"].unique().tolist()
            if not candidates:
                print(f"  ❌ No valid weekly candidate for {day.date()} {shift_time}")
                break

            selected = pick_random_staff(candidates, exclude=None)
            if not selected:
                print(f"  ❌ No available candidate for {day.date()} {shift_time}")
                break

            assign_one_agent(df_schedule, day, shift_time, selected)
            print(f"assigning {selected} to {shift_time} shift on {day.day_name()}")

    return df_schedule

def assign_needed_smt(df_schedule, day, shift, smt_needed, filter_func = None):
    # Filter SMT candidates who are not already assigned to this shift
    available_smt = df_schedule[
        (df_schedule["trained_social"] == True) &
        (df_schedule["Day"] == day) &
        (df_schedule["shift_time"].isna())
    ]

    smt_language = df_schedule[
        (df_schedule["trained_social"] == True) &
        (df_schedule["Day"] == day) &
        (df_schedule["shift_time"] == shift)
    ]["team"].tolist()

    if available_smt.empty:
        print(f"❌ No SMT agents available for shift {shift} on {day}.")
        return df_schedule

    if len(available_smt) < smt_needed:
        print(f"⚠️ Not enough SMT agents for shift {shift} on {day}. Needed: {smt_needed}, Available: {len(available_smt)}")
        return df_schedule

    # Sample SMTs and check language diversity
    selected_smt = available_smt.sample(n=smt_needed)
    selected_lang = selected_smt["team"].tolist()

    if not any(lang in smt_language for lang in selected_lang):
        for _, smt_row in selected_smt.iterrows():
            assign_one_agent(df_schedule, day, shift, smt_row["Employee"])
        print(f"✅ Assigned SMTs to shift {shift} on {day}:", selected_smt["Employee"].tolist())
    else:
        print(f"⚠️ Language overlap detected — skipping SMT assignment for shift {shift} on {day}.")
    selected_lang
    return df_schedule, selected_lang

def assign_one_agent(df_schedule, day, shift, agent):
    try:
        if isinstance(day, str):  # If passed "Monday", pick that day by weekday name
            mask = (
                (df_schedule["Employee"] == agent) &
                (df_schedule["Day"].dt.day_name() == day)
            )
        else:  # If passed a datetime/date, match directly
            mask = (
                (df_schedule["Employee"] == agent) &
                (df_schedule["Day"] == pd.to_datetime(day))
            )
        df_schedule.loc[mask, "shift_time"] = shift
    except Exception as e:
        print(f"⚠️ Error assigning agent {agent} to {day} shift {shift}: {e}")

def classify_shift(shift):
    try:
        shift_int = int(shift)
    except (ValueError, TypeError):
        return None

    if 11 <= shift_int <= 14:
        return "early_late"
    elif 14 < shift_int <= 17:
        return "late_late"
    elif 8 <= shift_int < 11:
        return "morning"
    else:
        return None

def assign_saturday_shifts(df_week, df_history, df_team, grouped_smt_team, shift_requirements, top_n_saturday=3):
    print("✅ START assign_saturday_shifts")
    week = df_week["Day"].dt.isocalendar().week.iloc[0]
    used_languages_late = set()

    already_lated = df_week.loc[
        (df_week["shift_time"].isin(["13", "15"])) &
        (df_week["Day"].dt.day_name() != "Saturday")
    ]["Employee"].unique().tolist()

    already_lated = set(already_lated)
    saturday_eligible = df_team.loc[
        ~df_team["name"].isin(already_lated),
        "name"
    ].tolist()
    print("📊 All SMT candidates available:")
    print(df_team[df_team["trained_social"] == True][["name", "team"]])

    sat_late_late_team = []
    sat_early_late_team = []
    requirement_15 = shift_requirements.loc["15", "min_required"]
    requirement_13 = shift_requirements.loc["13", "min_required"]

    # 1. Assign Saturday 15:00 (late-late)
    while len(sat_late_late_team) < requirement_15:
        smt_requirement = shift_requirements.loc["15", "smt_needed"]

        raw_smt = df_team[df_team["trained_social"] == True]["name"].tolist()
        print("👀 SMTs available BEFORE fairness check:", raw_smt)
        print("🧾 Counts in df_history:")
        print(df_history[df_history["name"].isin(raw_smt)][["name", "count_saturday_late_late"]])
        fair_sat15 = select_fairest(
            df_history, df_team, saturday_eligible,
            shift_type="saturday_late_late",
            top_n=top_n_saturday,
            required_count=smt_requirement,
            filter_func=lambda row: row["team"] not in used_languages_late and row["trained_social"],
            duplicate_teams=False
        )
        print("✅ SMTs selected for fairness (Sat 15):", fair_sat15["name"].tolist())

        if fair_sat15.empty:
            print("❌ No eligible candidate for Saturday 15:00 (late-late)")
            print("🔁 RETURNING EMPTY LISTS")
            return df_week, [], []
        name = fair_sat15["name"].iloc[0]
        lang = df_team.loc[df_team["name"] == name, "team"].iloc[0]
        used_languages_late.add(lang)
        sat_late_late_team.append(name)

    # 2a. Assign one SMT for 13:00 (early-late)
    if requirement_13 > 0:
        smt_requirement = shift_requirements.loc["13", "smt_needed"]
        raw_smt = df_team[df_team["trained_social"] == True]["name"].tolist()
        print("👀 SMTs available BEFORE fairness check:", raw_smt)
        print("🧾 Counts in df_history:")
        print(df_history[df_history["name"].isin(raw_smt)][["name", "count_saturday_late_late"]])

        fair_smt = select_fairest(
            df_history, df_team, saturday_eligible,
            shift_type="saturday_early_late",
            top_n=top_n_saturday,
            required_count=smt_requirement,
            filter_func=lambda row: row["team"] not in used_languages_late and row["trained_social"],
            duplicate_teams=False
        )
        print("✅ SMTs selected for fairness (Sat 13):", fair_smt["name"].tolist())

        if fair_smt.empty:
            print("❌ No SMT candidate for Saturday 13:00 (early-late)")
            print("🔁 RETURNING EMPTY LISTS")
            return df_week, [], []
        smt_name = fair_smt["name"].iloc[0]
        lang = df_team.loc[df_team["name"] == smt_name, "team"].iloc[0]
        used_languages_late.add(lang)
        sat_early_late_team.append(smt_name)
    print("📉 SMTs after filtering for 15:00:", fair_sat15["name"].tolist())
    print("📉 SMTs after filtering for 13:00:", fair_smt["name"].tolist())

    # 2b. Fill remaining 13:00 slots
    while len(sat_early_late_team) < requirement_13:
        raw_smt = df_team[df_team["trained_social"] == True]["name"].tolist()
        print("👀 SMTs available BEFORE fairness check:", raw_smt)
        print("🧾 Counts in df_history:")
        print(df_history[df_history["name"].isin(raw_smt)][["name", "count_saturday_late_late"]])

        fair_sat13 = select_fairest(
            df_history, df_team, saturday_eligible,
            shift_type="saturday_early_late",
            top_n=top_n_saturday,
            required_count=1,
            filter_func=lambda row: row["team"] not in used_languages_late,
            duplicate_teams=False
        )
        if fair_sat13.empty:
            print("❌ No non-SMT candidate for Saturday 13:00 (early-late)")
            print("🔁 RETURNING EMPTY LISTS")
            return df_week, [], []
        name = fair_sat13["name"].iloc[0]
        lang = df_team.loc[df_team["name"] == name, "team"].iloc[0]
        used_languages_late.add(lang)
        sat_early_late_team.append(name)

    # 3. Assign shifts
    for name in sat_late_late_team:
        days = df_week[(df_week["Employee"] == name) &
                       (df_week["Day"].dt.day_name() == "Saturday")]["Day"]
        for d in days:
            assign_one_agent(df_week, d, "15", name)

    for name in sat_early_late_team:
        days = df_week[(df_week["Employee"] == name) &
                       (df_week["Day"].dt.day_name() == "Saturday")]["Day"]
        for d in days:
            assign_one_agent(df_week, d, "13", name)

    print("✅ END assign_saturday_shifts")
    print("🔁 RETURNING NORMAL VALUES")
    return df_week, sat_late_late_team, sat_early_late_team

def assign_saturdays_and_rdo(df_week, df_team, shift_requirements, df_history, sat_late_late_team, sat_early_late_team):
    # ────────── Step 1: Assign Saturday morning ("9") SMT ──────────
    sat_morning_team = set()
    languages_morning_team = set()
    morning_shift = "9"
    morning_smt_needed = shift_requirements.loc["9", "smt_needed"]  # number of SMT needed for Saturday morning
    df_week = assign_needed_smt(df_week, "Saturday", morning_shift, morning_smt_needed)

    requirement_mor = shift_requirements.loc["9", "min_required"]
    day_mask = df_week["Day"].dt.day_name() == "Saturday"
    working_mask = df_week["shift_time"].isin(["RDO", None])
    combined_mask = day_mask & working_mask
    morning_rows = df_week[combined_mask]["Employee"]
    candidates_mor = morning_rows.tolist()

    sat_morning_team = select_fairest(
        df_history,
        df_team,
        candidates_mor,
        shift_type="saturday_morning",
        top_n=requirement_mor * 2,
        required_count=requirement_mor,
        duplicate_teams=False
    )

    for agent in sat_morning_team["name"]:
        assign_one_agent(df_week, "Saturday", morning_shift, agent)
        team_row = df_team.loc[df_team["name"] == agent, "team"]
        if not team_row.empty:
            languages_morning_team.add(team_row.values[0])
        else:
            print(f"⚠️ Agent '{agent}' not found in df_team — cannot retrieve team info.")

    # ────────── Step 2: Gather all Saturday‐involved agents ──────────
    all_saturday_agents = set(sat_early_late_team + sat_late_late_team + list(sat_morning_team))
    possible_day_off = ["Wednesday", "Thursday", "Friday"]

    # ────────── Step 3: For each Saturday agent, assign one mid‐week RDO and fill missing Mon–Fri with "9" ──────────
    for i, agent in enumerate(all_saturday_agents):
        # 3a. Pick a rotating weekday to give as RDO
        day_off = possible_day_off[i % len(possible_day_off)]
        df_week.loc[
            (df_week["Employee"] == agent) &
            (df_week["Day"].dt.day_name() == day_off),
            "shift_time"
        ] = "RDO"

        # 3b. Check Mon–Fri actual vs. expected workdays, fill missing with "9"
        mon_to_fri = df_week[
            (df_week["Employee"].str.strip() == agent.strip()) &
            (df_week["Day"].dt.weekday < 5)
        ]

        actual_workdays = mon_to_fri[
            ~mon_to_fri["shift_time"].isin(["RDO", "OFF", "AL", None])
        ].shape[0]

        # Safely look up days_per_week (strip whitespace first)
        agent_clean = agent.strip()
        team_row = df_team[df_team["name"].str.strip() == agent_clean]

        if team_row.empty:
            print(f"⚠️ Agent '{agent}' not found in df_team; skipping their days_per_week lookup")
            continue

        expected_workdays = int(team_row["days_per_week"].values[0])

        if actual_workdays < expected_workdays:
            needed = expected_workdays - actual_workdays
            missing_idx = mon_to_fri[mon_to_fri["shift_time"].isna()].index[:needed]
            df_week.loc[missing_idx, "shift_time"] = "9"

    # ────────── Step 4: Debug print any still‐missing Mon–Fri slots (should be none) ──────────
    missing_workdays = df_week[
        (df_week["Day"].dt.day_name().isin(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])) &
        (df_week["shift_time"].isna())
    ]
    print("\n📅 Missing Mon–Fri shift assignments before fallback to '9':")
    print(missing_workdays[["Employee", "Day", "shift_time"]])

    # ────────── Step 5: Finally set any leftover Saturday slots to "RDO" ──────────
    df_week.loc[
        (df_week["Day"].dt.day_name() == "Saturday") &
        (df_week["shift_time"].isna()),
        "shift_time"
    ] = "RDO"

    print("\n🧮 Days worked just before enforce_max_days_off():")
    print_avg_days_worked(df_week)

    return df_week, sat_morning_team, sat_late_late_team, sat_early_late_team

# In enforce_max_days_off()
def enforce_max_days_off(df_schedule, df_team):
    for _, row in df_team.iterrows():
        agent = row["name"]
        max_rdo_per_week = 7 - row["days_per_week"]
        agent_schedule = df_schedule[df_schedule["Employee"] == agent].copy()
        agent_schedule["temp_week"] = agent_schedule["Day"].dt.isocalendar().week

        for week, week_data in agent_schedule.groupby("temp_week"):
            # Count how many days this agent actually worked (excluding RDO/OFF/AL/None)
            actual_days_worked = week_data[
                ~week_data["shift_time"].isin(["RDO", "OFF", "AL", None])
            ].shape[0]
            expected_days = row["days_per_week"]

            # If already met or exceeded expected days, skip
            if actual_days_worked >= expected_days:
                continue

            rdo_count = week_data[week_data["shift_time"] == "RDO"].shape[0]
            rdo_balance = max_rdo_per_week - rdo_count

            if rdo_balance < 0:
                # Too many RDOs—convert excess RDOs back to "9"
                rdo_rows = week_data[week_data["shift_time"] == "RDO"]
                rows_to_change = rdo_rows.sample(n=abs(rdo_balance)).index
                for idx in rows_to_change:
                    target_day = df_schedule.loc[idx, "Day"]
                    assign_one_agent(df_schedule, target_day.day_name(), "9", agent)

            elif rdo_balance > 0:
                # We need to add RDOs. First look for Saturday shifts to convert.
                saturday_work = week_data[
                    (week_data["shift_time"] != "RDO") &
                    (week_data["Day"].dt.day_name() == "Saturday")
                ]

                if len(saturday_work) < rdo_balance:
                    # Not enough Saturdays to convert, include other working days
                    extra_work = week_data[
                        ~week_data["shift_time"].isin(["RDO", "OFF", "AL"]) &
                        (week_data["Day"].dt.day_name() != "Saturday")
                    ]
                    saturday_work = pd.concat([saturday_work, extra_work])

                # Now filter that combined set to Mon–Fri only
                weekday_candidates = saturday_work[saturday_work["Day"].dt.weekday < 5]

                if len(weekday_candidates) >= rdo_balance:
                    rows_to_change = weekday_candidates.sample(n=rdo_balance).index
                    for idx in rows_to_change:
                        target_day = df_schedule.loc[idx, "Day"]
                        assign_one_agent(df_schedule, target_day.day_name(), "RDO", agent)
                else:
                    print(f"⚠️ Not enough working rows to assign RDOs for agent {agent}")

    return df_schedule

def smt_check(df_schedule, df_team):
    team_shifts = ["9", "13", "15"]
    smt_team = df_team[df_team["trained_social"] == True]["name"].tolist()
    grouped = df_schedule.groupby(["Day", "shift_time"])
    smts_present = []
    for (day, shift), group_df in grouped:
        if shift not in team_shifts:
            continue
        scheduled_agents = group_df["Employee"].tolist()
        smts_present = [agent for agent in scheduled_agents if agent in smt_team]

    if len(smts_present) >= 2:
        print(f"✅ Enough SMT on {day} ({shift}): {len(smts_present)}")
    else:
        print(f"❌ Not enough SMT on {day} ({shift}): {len(smts_present)}")

def report_smt_coverage(df_schedule, df_team):

    smt_team = df_team[df_team["trained_social"] == True]["name"].tolist()
    report_df = df_schedule[df_schedule["Employee"].isin(smt_team)]
    pivot = report_df.pivot_table(index = "shift_time", columns = "Day", values="Employee", aggfunc = "count", fill_value = 0)
    print("SMT report created!")
    return pivot

def check_and_fill_smt(df_schedule, shift_requirements, df_team):
    smt_needed = {
        "9": 2,
        "15": 1
    }

    smt_agents = df_team[df_team["trained_social"] == True]["name"].tolist()

    for day in df_schedule["Day"].unique():
        if pd.to_datetime(day).weekday() == 6:
            print(f"⛔ Skipping SMT assignment on Sunday: {day}")
            continue

        for shift in ["9", "15"]:
            day_shift_mask = (df_schedule["Day"] == day) & (df_schedule["shift_time"] == shift)
            assigned_smt_count = df_schedule.loc[day_shift_mask & df_schedule["trained_social"], "Employee"].nunique()
            needed = smt_needed[shift] - assigned_smt_count

            if needed > 0:
                pool = df_schedule[
                    (df_schedule["Day"] == day) &
                    (df_schedule["Employee"].isin(smt_agents)) &
                    (df_schedule["shift_time"].isna())
                ]

                print(f"⚠ {day} {shift}: Need {needed} more SMT staff.")

                for agent in pool["Employee"].unique()[:needed]:
                    assign_one_agent(df_schedule, day, shift, agent)
                    print(f"{agent} added to {day} at {shift}")

    return df_schedule

def apply_weekly_shift_logic(df_schedule, df_team, grouped_smt_team, shift_requirements, df_history):
    df_schedule["week_id"] = df_schedule["Day"].dt.strftime("%Y-W%U")  # e.g. '2025-W20'
    all_weeks = sorted(df_schedule["week_id"].unique())

    for week in all_weeks:
        print(f"\n🗖 Assigning shifts for week {week}")
        weekly_mask = df_schedule["week_id"] == week
        df_week = df_schedule[weekly_mask].copy()
        df_week["Weekday"] = df_week["Day"].dt.weekday

        # 1. Assign weekday late-late and early-late shifts
        df_week, late_late_team, early_late_team = assign_both_late_shifts(
            df_week, df_history, df_team, grouped_smt_team, shift_requirements
        )

        # 🔥 FIX: immediately update history for weekday shifts
        df_history = update_history(
            df_history, df_week,
            weekday_late_late=late_late_team,
            weekday_early_late=early_late_team
        )
        result = assign_saturday_shifts(
            df_week,
            df_history,
            df_team,
            grouped_smt_team,
            shift_requirements,
            top_n_saturday=3
        )

        if result is None:
            raise RuntimeError("❌ assign_saturday_shifts() returned None unexpectedly!")

        df_week, sat_late_late_team, sat_early_late_team = result

        # 2. Assign Saturday shifts
        # … after weekday shifts and history update …
        # … after updating weekday history …
        df_week, sat_morning_team, sat_late_late_team, sat_early_late_team = assign_saturdays_and_rdo(
            df_week,
            df_team,
            shift_requirements,
            df_history,
            sat_late_late_team,
            sat_early_late_team
        )
        print("🧪 SMT assigned to Sat 15:", sat_late_late_team)
        print("🧪 SMT assigned to Sat 13:", sat_early_late_team)
        print("🧪 Sat morning team:", sat_morning_team)
        print(df_week[df_week["Day"].dt.dayofweek == 5][["Day", "Employee", "shift_time"]])

        # 🔥 FIX: update history for Saturday shifts too
        df_history = update_history(
            df_history, df_week,
            saturday_late_late=sat_late_late_team,
            saturday_early_late=sat_early_late_team,
            saturday_morning=sat_morning_team
        )
        print("📈 History counts after update:\n",
              df_history[["name", "count_saturday_late_late"]].sort_values("count_saturday_late_late", ascending=False))

        # 3. Apply staffing rules
        df_week = enforce_minimum_staffing(df_week, shift_requirements)
        df_week = enforce_max_days_off(df_week, df_team)
        df_week = check_and_fill_smt(df_week, shift_requirements, df_team)

        # 4. Write back into global schedule
        df_schedule.loc[weekly_mask] = df_week

        print(df_history[["name", "count_late_late", "latest_late_late"]].sort_values("count_late_late",
                                                                                      ascending=False).head(5))

        # Update history with all current week assignments

    print("\n📅 Weekly shift logic complete for all weeks.")
    return df_history, df_schedule

def output_schedule(excel_file_path, destination_file_path):
    wb = openpyxl.load_workbook(excel_file_path)
    # 🗓️ Step 1: Get next and previous months
    try:
        # Extract all sheets that look like "Month YYYY"
        schedule_tabs = []
        for name in wb.sheetnames:
            try:
                parsed = datetime.strptime(name, "%B %Y")
                schedule_tabs.append(parsed)
            except ValueError:
                continue  # Skip tabs that don't match

        if not schedule_tabs:
            raise ValueError("No valid schedule tabs found")

        # Get the most recent tab
        prev_month = max(schedule_tabs)

        # Compute next month manually
        if prev_month.month == 12:
            next_month = datetime(prev_month.year + 1, 1, 1)
        else:
            next_month = datetime(prev_month.year, prev_month.month + 1, 1)

    except Exception as e:
        print(f"Error {e}: no previous schedule found, defaulting to current month")
        today = datetime.today()
        if today.month == 12:
            next_month = datetime(today.year + 1, 1, 1)
            prev_month = datetime(today.year, 12, 1)
        else:
            next_month = datetime(today.year, today.month + 1, 1)
            prev_month = datetime(today.year, today.month, 1)

    next_sheet_name = next_month.strftime("%B %Y")
    prev_sheet_name = prev_month.strftime("%B %Y")
    base_sheet_name = "Base schedule"
    #define next year and month
    year = next_month.year
    month = next_month.month

    # 📁 Step 2: Load workbook and create new sheet from base
    # 📁 Step 2: Load workbook and create new sheet from base
    if base_sheet_name not in wb.sheetnames:
        raise RuntimeError(f"❌ Base sheet '{base_sheet_name}' not found! Sheets found: {wb.sheetnames}")

    try:
        base_sheet = wb[base_sheet_name]
        new_sheet = wb.copy_worksheet(base_sheet)
        new_sheet.title = next_sheet_name
        print(f"📄 Sheetnames before saving: {wb.sheetnames}")
        print(f"✅ Copied new tab: {new_sheet.title}")
    except Exception as e:
        raise RuntimeError(f"❌ Failed to copy base sheet: {e}")

    print("✅ Available sheets:", wb.sheetnames)

    # 📅 Step 3: Determine start and end date
    try:
      previous_sheet = wb[prev_sheet_name]
      for col in reversed(range(3, previous_sheet.max_column + 1)):
        val = previous_sheet.cell(row=1, column=col).value
        if isinstance(val, (datetime, date)):
          previous_sheet_last_date = val
          break
    except KeyError as e:
      print(f"Error {e}: no previous month tab")
      first_day = datetime(year, month, 1)
      previous_sheet_last_date = first_day - timedelta(days=1)


    start_date = previous_sheet_last_date + timedelta(days=1)
    end_of_month = datetime(year, month, calendar.monthrange(year, month)[1]).date()
    end_date = end_of_month + timedelta(days=(6 - end_of_month.weekday()))

    # 📆 Step 4: Write full range of dates into row 1 (starting column 3)
    for i, current_date in enumerate(pd.date_range(start=start_date, end=end_date)):
        new_sheet.cell(row=1, column=3 + i).value = current_date.date()

    # 📋 Step 5: Fill weekday names in each "team" row
    for row in range(1, new_sheet.max_row + 1):
        team_cell = new_sheet.cell(row=row, column=1)
        if team_cell.value and "team" in str(team_cell.value).strip().lower():
            print(f"Found team row: {row} → {team_cell.value}")
            for col in range(3, new_sheet.max_column + 1):
                date_cell = new_sheet.cell(row=1, column=col).value
                if isinstance(date_cell, (datetime, date)):
                    weekday = date_cell.strftime("%A")
                    new_sheet.cell(row=row, column=col).value = weekday
                else:
                    print(f"Warning: cell at row=1, col={col} is not a valid date.")

    #step 6: Fill the formulas at the bottom of the sheet
    latest_row = new_sheet.max_row

    for col in range(3, new_sheet.max_column + 1):
        for row in range(latest_row - 3, latest_row):
            col_letter = get_column_letter(col)
            table_end = latest_row - 5
            new_sheet.cell(row=row, column=col).value = f"=COUNTIF({col_letter}3:{col_letter}{table_end},B{row})"

        new_sheet.cell(row=latest_row, column=col).value = (f"={col_letter}{latest_row-3}+"
                                                                     f"{col_letter}{latest_row-2}+"
                                                                     f"{col_letter}{latest_row-1}")
    #STEP 7: Conditional formatting
    rrdo_fill = PatternFill(start_color="FF707071", end_color="FF707071", fill_type="solid")
    rrdo_rule = CellIsRule(operator='equal', formula=['"RRDO"'], fill=rrdo_fill)
    rdo_fill = PatternFill(start_color="FF908e92", end_color="FF908e92", fill_type="solid")
    rdo_rule = CellIsRule(operator='equal', formula=['"RDO"'], fill=rdo_fill)
    morning_fill = PatternFill(start_color="FFf9f7fb", end_color="FFf9f7fb", fill_type="solid")
    morning_rule = FormulaRule(formula=['LEFT(C3, 2)="09"'], fill=morning_fill)
    early_late_fill = PatternFill(start_color="FFc27aff", end_color="FFc27aff", fill_type="solid")
    early_late_rule = FormulaRule(formula=['LEFT(C3, 2)="13"'], fill=early_late_fill)
    late_late_fill = PatternFill(start_color="FF8903fb", end_color="FF8903fb", fill_type="solid")
    late_late_rule = FormulaRule(formula=['LEFT(C3, 2)="15"'], fill=late_late_fill)
    al_fill = PatternFill(start_color="FF2596be", end_color="FF2596be", fill_type="solid")
    al_rule = CellIsRule(operator='equal', formula=['"AL"'], fill=al_fill)
    off_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    off_font = Font(color="FFFFFF")  # white text

    off_rule = FormulaRule(formula=[f'LOWER(C3)="off"'], fill=off_fill, font=off_font)
    range_str = f"C3:{get_column_letter(new_sheet.max_column)}{new_sheet.max_row}"

    new_sheet.conditional_formatting.add(range_str, rdo_rule)
    new_sheet.conditional_formatting.add(range_str, rrdo_rule)
    new_sheet.conditional_formatting.add(range_str, morning_rule)
    new_sheet.conditional_formatting.add(range_str, early_late_rule)
    new_sheet.conditional_formatting.add(range_str, late_late_rule)
    new_sheet.conditional_formatting.add(range_str, al_rule)
    new_sheet.conditional_formatting.add(range_str, off_rule)


    # 💾 Step 8: Save updated file
    wb.save(destination_file_path)

    # 🧪 Reload and verify the sheet was actually created
    wb = openpyxl.load_workbook(destination_file_path)
    if next_sheet_name not in wb.sheetnames:
        raise RuntimeError(f"❌ Tab '{next_sheet_name}' was not saved correctly in {destination_file_path}")

    # ✅ Return metadata
    sheet_name = next_sheet_name
    return sheet_name, start_date, end_date, pd.date_range(start=start_date, end=end_date)

import sys

def dbg(*args):
    print(*args, flush=True, file=sys.stdout)

# then replace every print(...) in load_employee_history with dbg(...)
def load_employee_history(file_path):

    from dateutil.parser import parse

    print("📥 load_employee_history: starting with", file_path)
    unrecognized_cells = []

    # ─── 1) Try to load the History sheet ────────────────────────────────
    try:
        df = pd.read_excel(file_path, sheet_name="History", usecols="B:L")
        print("✅ History sheet read, shape:", df.shape)
    except Exception as e:
        print(f"⚠️ Cannot read History sheet — {e}")
        print("   falling back to Team sheet for history initialization")
        try:
            df = pd.read_excel(file_path, sheet_name="Team", usecols="B:L")
            print("✅ Team sheet read instead, shape:", df.shape)
            # drop entirely blank rows
            df = df.dropna(how="all")
        except Exception as e2:
            dbg(f"❌ Cannot read Team sheet either — {e2}")
            raise RuntimeError("No History or Team sheet found") from e2

    # ─── 2) Clean & normalize df_history ─────────────────────────────────
    df.columns = df.columns.str.strip().str.lower()
    # rename agent's name → name if present
    if "agent's name" in df.columns:
        df = df.rename(columns={"agent's name": "name"})
        dbg("   renamed \"agent's name\" → \"name\"")
    else:
        dbg("   warning: no \"agent's name\" column to rename")

    # ensure all the count_... cols exist as ints
    zero_cols = [
        "count_early_late", "count_late_late",
        "count_saturday_morning", "count_saturday_early_late", "count_saturday_late_late"
    ]
    for c in zero_cols:
        df[c] = df.get(c, 0).fillna(0).astype(int)
    dbg("   ensured zero-count columns")

    # ensure all the latest_... cols exist as object
    latest_cols = [
        "latest_early_late", "latest_late_late",
        "latest_saturday_morning", "latest_saturday_early_late", "latest_saturday_late_late"
    ]
    for c in latest_cols:
        df[c] = df.get(c, None)
    dbg("   ensured latest-date columns")

    # fill days_per_week default if missing
    if "days_per_week" not in df.columns or df["days_per_week"].isnull().any():
        if "days_per_week" not in df.columns:
            df["days_per_week"] = 5
        else:
            df["days_per_week"] = df["days_per_week"].fillna(5).astype(int)

        dbg("   filled days_per_week default = 5")

    # strip & lower the names so matching always works
    if "name" in df.columns:
        df["name"] = df["name"].astype(str).str.strip().str.lower()
        dbg("   normalized names")
    else:
        dbg("   warning: no \"name\" column found after renaming")

    # ─── 3) Grab the latest schedule sheet to return ──────────────────────
    wb = openpyxl.load_workbook(file_path, data_only=True)
    sheet_dates = []
    for name in wb.sheetnames:
        try:
            dt = parse(name)
            sheet_dates.append((name, dt))
        except:
            pass
    if not sheet_dates:
        raise RuntimeError("No date-formatted sheets found to pick latest schedule from")
    sheet_dates.sort(key=lambda x: x[1])
    latest_name = sheet_dates[-1][0]
    schedule_sheet = wb[latest_name]
    dbg(f"✅ Using latest schedule tab: \"{latest_name}\"")

    # ─── 4) Done! ────────────────────────────────────────────────────────
    dbg(f"   🎯 load_employee_history complete, returning df({df.shape}), "
          f"cells({len(unrecognized_cells)}), sheet({latest_name})")
    return df, unrecognized_cells, schedule_sheet

def check_mismatches(excel_file_path, df_history, schedule_sheet, df_team):
    """
    Compares names across different tabs:
    - Team tab
    - History tab
    - Latest schedule sheet
    - Holiday sheet (optional)

    Returns a dictionary of mismatches.
    """
    previous_schedule_sheet = schedule_sheet  # to be returned from the load employee function
    base_schedule_sheet = pd.read_excel(excel_file_path, sheet_name="Base schedule", usecols="A:B")
    from openpyxl.utils import get_column_letter

    # Start with openpyxl
    wb = openpyxl.load_workbook(excel_file_path, data_only=True)

    if "Holidays" not in wb.sheetnames:
        print("❌ 'Holidays' sheet not found in workbook.")
        holiday_sheet = pd.DataFrame()  # or handle differently if needed
    else:
        ws = wb["Holidays"]

        # Find last used column in row 1
        last_used_col = 2
        for col in range(2, ws.max_column + 1):
            if ws.cell(row=1, column=col).value:
                last_used_col = col

        from openpyxl.utils import get_column_letter
        end_col_letter = get_column_letter(last_used_col)
        usecols = f"B:{end_col_letter}"
        print(f"✅ Reading Holidays with usecols='{usecols}'")

        holiday_sheet = pd.read_excel(excel_file_path, sheet_name="Holidays", usecols=usecols)

    team_names = [str(name).strip().lower() for name in df_team["name"].dropna()]
    previous_schedule_names = []
    history_names = [str(name).strip().lower() for name in df_history["name"].dropna()]
    holiday_names = []
    base_schedule_names = []

    # Finding the previous schedule names
    for row in previous_schedule_sheet.iter_rows(min_col=2, max_col=2):
        for cell in row:
            name = cell.value
            if name:
                previous_schedule_names.append(name.strip().lower())

    # Finding the holiday names (from row 1)
    holiday_names = [
        str(val).strip().lower()
        for val in holiday_sheet.iloc[0, :]
        if pd.notna(val) and str(val).strip()
    ]

    # Finding the base schedule names
    for idx, row in base_schedule_sheet.iterrows():
        for cell in row:
            if cell:
                base_schedule_names.append(str(cell).strip().lower())

    leavers = set(previous_schedule_names) - set(team_names)
    new_hires = set(team_names) - set(previous_schedule_names)
    no_history = set(team_names) - set(history_names)
    history_only = set(history_names) - set(team_names)
    missing_in_team = set(base_schedule_names) - set(team_names)
    missing_in_base_schedule = set(team_names) - set(base_schedule_names)
    missing_in_holiday = set(team_names) - set(holiday_names)
    missing_from_holiday = set(holiday_names) - set(team_names)

    mismatches = {
        "In previous months but not team sheet": leavers,
        "In the team sheet but not in the previous months": new_hires,
        "People in the team sheet not in the history sheet": no_history,
        "People in the history sheet not in the base schedule": history_only,
        "people in the base schedule but not in the team sheet": missing_in_team,
        "people in the team sheet but not in the base schedule": missing_in_base_schedule,
        "people in the holiday sheet but not in the team sheet": missing_from_holiday,
        "people in the team sheet but not in the holiday sheet": missing_in_holiday,
    }

    return mismatches


def fill_schedule(df_team, df_schedule, destination_file_path, sheet_name):
    print(f"📂 Filling schedule into: {destination_file_path}")
    print("📋 Sheet name:", sheet_name)
    print("📊 df_schedule shape:", df_schedule.shape)
    print("👥 df_schedule Employees:", df_schedule['Employee'].unique())
    wb = openpyxl.load_workbook(destination_file_path)
    sheet = wb[sheet_name]
    staff_list = df_team["name"].tolist()

    for staff in staff_list:
        df_agent = df_schedule[df_schedule["Employee"] == staff]
        agent_row = df_team[df_team["name"] == staff]

        if agent_row.empty:
            print(f"⚠️ No schedule for: {staff}")
            continue  # skip if agent is not found

        hours_per_week = agent_row["hours_per_week"].values[0]
        days_per_week = agent_row["days_per_week"].values[0]

        row_to_fill = None
        for row in sheet.iter_rows(min_row=3, max_col=2):
            if row[1].value == staff:
                row_to_fill = row[1].row
                break

        if row_to_fill is None:
            print(f"⚠️ Could not find row for: {staff}")
            continue

        for col in range(3, sheet.max_column + 1):
            cell = sheet.cell(row=1, column=col)
            wb_date = cell.value

            if wb_date is None:
                break  # no date = no shift

            if not isinstance(wb_date, (datetime, date)):
                continue  # skip non-date headers

            wb_day = pd.to_datetime(wb_date).date()
            for _, row in df_agent.iterrows():
                row_day = pd.to_datetime(row["Day"]).date()
                if wb_day == row_day:
                    shift = row["shift_time"]

                    if isinstance(shift, str) and shift.isdigit():
                        start_hour = int(shift)

                        # use 5 days for late shifts
                        if classify_shift(shift) in {"early_late", "late_late"}:
                            days_used = 5
                        else:
                            days_used = days_per_week

                        hours_per_day = hours_per_week / days_used
                        end_hour = start_hour + hours_per_day + 1

                        shift_str = f"{start_hour:02.0f}:00 - {int(end_hour):02.0f}:00"
                        sheet.cell(row=row_to_fill, column=col).value = shift_str
                    else:
                        sheet.cell(row=row_to_fill, column=col).value = shift
    print("✅ Writing file to:", destination_file_path)
    print("✅ Schedule preview:\n", df_schedule.head(10))

    wb.save(destination_file_path)

def fill_history_tab(df_history, excel_file_path, sheet_name="History", all_months=False):
    print("📥 fill_history_tab called with:", excel_file_path)
    try:
        wb = openpyxl.load_workbook(excel_file_path, data_only=True)
        print("   ✅ workbook loaded, sheets:", wb.sheetnames)
    except Exception as e:
        print("   ❌ fill_history_tab: cannot open workbook:", e)
        raise

    dbg("   sheets found:", wb.sheetnames)
    # 1) Parse every “Month YYYY” tab
    parsed_tabs = []
    today = datetime.today().replace(day=1)

    for name in wb.sheetnames:
        try:
            parsed_date = datetime.strptime(name, "%B %Y")
            parsed_tabs.append((name, parsed_date))

        except ValueError:
            continue
    valid_tabs = [(n, d) for n, d in parsed_tabs if d < today]

    print("Parsed tabs:", parsed_tabs)
    print("Today is:", today)
    print("Valid tabs:", valid_tabs)

    # 2) Bail out if there were no date-named tabs at all
    if not parsed_tabs:
        raise ValueError("No valid schedule tabs found in the workbook.")

    # 3) Filter out current/future months, keep only past
    if not valid_tabs:
        raise ValueError("No past schedule tabs to process.")
    # 4) Sort the filtered list by date
    valid_tabs.sort(key=lambda x: x[1])

    # 5) Decide which tab(s) to process
    if all_months:
        tabs_to_process = [name for name, _ in valid_tabs]
    else:
        tabs_to_process = [valid_tabs[-1][0]]  # only the most‐recent past month

    # 6) Prepare to update the History sheet
    history_sheet = wb[sheet_name]
    staff_list   = df_history["name"].tolist()

    # Map header names → column indices
    col_indices = {
        history_sheet.cell(row=1, column=col).value: col
        for col in range(1, history_sheet.max_column + 1)
    }
    # Sanity-check that all required headers exist
    required = [
        "latest_early_late", "count_early_late",
        "latest_late_late",  "count_late_late",
        "latest_saturday_morning",  "count_saturday_morning",
        "latest_saturday_early_late", "count_saturday_early_late",
        "latest_saturday_late_late",  "count_saturday_late_late"
    ]
    for header in required:
        assert header in col_indices, f"Missing History header: {header}"

    # 7) Process each selected tab
    dbg("   processing History tab rows …")
    for tab in tabs_to_process:
        print(f"Checking tab: {tab}")
        schedule_sheet = wb[tab]
        for staff in staff_list:
            # find the row in History and in Schedule
            row_to_fill = next(
                (r[1].row for r in history_sheet.iter_rows(min_row=2, max_col=2)
                 if r[1].value == staff),
                None
            )
            row_to_pull = next(
                (r[1].row for r in schedule_sheet.iter_rows(min_row=3, max_col=2)
                 if r[1].value == staff),
                None
            )
            if row_to_fill is None or row_to_pull is None:
                continue
            print(f"Processing staff: {staff} → row_to_fill: {row_to_fill}, row_to_pull: {row_to_pull}")
            # collect shift dates by type
            shifts_13_weekday   = []
            shifts_13_saturday  = []
            shifts_15_weekday   = []
            shifts_15_saturday  = []
            shifts_9_saturday   = []

            for col in range(3, schedule_sheet.max_column + 1):
                shift = schedule_sheet.cell(row=row_to_pull, column=col).value
                if isinstance(shift, str) and ":" in shift:
                    shift_start = shift.split(":")[0]
                else:
                    shift_start = shift  # keep original if it's a raw value like "AL"

                if shift_start not in {"13", "15", "9"}:
                    continue

                date_value   = schedule_sheet.cell(row=1, column=col).value
                weekday_name = date_value.strftime("%A")
                print(f"Shift {shift_start} on {date_value} for {staff}")
                if classify_shift(shift_start) == "early_late":
                    (shifts_13_saturday if weekday_name=="Saturday"
                     else shifts_13_weekday).append(date_value)
                elif classify_shift(shift_start) == "late_late":
                    (shifts_15_saturday if weekday_name=="Saturday"
                     else shifts_15_weekday).append(date_value)
                elif classify_shift(shift_start) == "morning" and weekday_name == "Saturday":
                    shifts_9_saturday.append(date_value)

            # helper to write latest and count once‐per‐week
            def update_column(dates, latest_key, count_key):
                if not dates:
                    return
                seen_weeks = set()
                final_dates = []
                for dt in dates:
                    wk = dt.isocalendar()[1]
                    if wk not in seen_weeks:
                        seen_weeks.add(wk)
                        final_dates.append(dt)
                if final_dates:
                    latest_shift = max(final_dates)
                    print(f"✅ Updating {staff} → {latest_key}: latest = {latest_shift}, count = {len(final_dates)}")

                # if we have any, write the max date & bump the count by #weeks
                latest_shift = max(final_dates)
                history_sheet.cell(row=row_to_fill,
                                   column=col_indices[latest_key]).value = latest_shift
                cell = history_sheet.cell(row=row_to_fill,
                                          column=col_indices[count_key])
                cell.value = (cell.value or 0) + len(final_dates)

            # apply to each shift‐type bucket
            update_column(shifts_13_weekday,   "latest_early_late",           "count_early_late")
            update_column(shifts_13_saturday,  "latest_saturday_early_late",  "count_saturday_early_late")
            update_column(shifts_15_weekday,   "latest_late_late",            "count_late_late")
            update_column(shifts_15_saturday,  "latest_saturday_late_late",   "count_saturday_late_late")
            update_column(shifts_9_saturday,   "latest_saturday_morning",     "count_saturday_morning")

    # 8) Save all updates back to the workbook
    print("✅ History updated and saved to", excel_file_path)

    # at very end of fill_history_tab
    wb.save(excel_file_path)
    print(f"✅ fill_history_tab: saved History into {excel_file_path!r}")


def print_avg_days_worked(df_schedule):
    temp = df_schedule.copy()
    temp["Week"] = temp["Day"].dt.isocalendar().week
    worked = temp[~temp["shift_time"].isin(["RDO", "OFF", "AL", None])]
    avg_days = (
        worked.groupby(["Employee", "Week"])
        .size()
        .groupby("Employee")
        .mean()
        .sort_values(ascending=False)
    )
    print("\n📊 Average Days Worked Per Week:")
    print(avg_days.round(2))
import os
import shutil

def run_schedule(uploaded_path, output_folder):
    # 1️⃣ Make a working copy right away
    os.makedirs(output_folder, exist_ok=True)
    dest = os.path.join(output_folder, "Team_updated.xlsx")
    shutil.copy(uploaded_path, dest)
    excel = dest
    print("🔨 Working copy is:", excel)
    # 2️⃣ Now load EVERYTHING from the copy
    shift_requirements = load_shift_requirements(excel, sheet_name="Shifts")

    df_team   = load_employee_list(excel)
    df_history, _, schedule_sheet = load_employee_history(excel)

    # 3️⃣ Update that copy’s History tab
    print("📥 run_schedule: about to call fill_history_tab…")
    fill_history_tab(df_history, excel)
    print("   🔍 Checking History[2,3]…", end=" ")

    wb2 = openpyxl.load_workbook(excel, data_only=True)
    val = wb2["History"].cell(row=2, column=3).value
    print(repr(val))

    # 4️⃣ Re-load history/sheet from the same copy to pick up your writes
    df_history, unrec_cells, schedule_sheet = load_employee_history(excel)

    # 5️⃣ Compute mismatches against that now-up-to-date copy
    mismatches = check_mismatches(excel,
                                  df_history,
                                  schedule_sheet,
                                  df_team)
    print("✅ run_schedule: mismatches =", mismatches)

    # 6️⃣ Append a new month tab to the copy
    sheet_name, start_date, end_date, full_date_range = output_schedule(
        excel,  # source for base & prev-month
        excel   # and also target for saving
    )

    # 7️⃣ Build your DataFrame and merge
    df_schedule = create_schedule_structure(
        df_holidays=None,
        employee_list=df_team["name"].tolist(),
        start_date=start_date,
        end_date=end_date
    )
    df_schedule = merge_df_team_and_df_schedule(df_team, df_schedule)
    df_schedule = assign_initial_shifts(df_schedule)

    # 8️⃣ Integrate holidays, assign shifts, etc...
    df_holidays      = load_holiday_calendar(excel, sheet_name="Holidays")
    df_holidays_long = process_holiday_calendar(df_holidays)
    if df_holidays_long is not None:
        df_schedule = integrate_holidays_into_schedule(df_schedule, df_holidays_long)

    _, grouped_smt_team = extract_smt_staff_grouped(excel)
    df_history, df_schedule = apply_weekly_shift_logic(
        df_schedule, df_team, grouped_smt_team, shift_requirements, df_history
    )
    # … plus your other assignment passes …

    print("🧪 df_schedule columns:", df_schedule.columns.tolist())
    print(df_history[["name", "count_saturday_late_late", "count_saturday_early_late"]].sort_values(
        by="count_saturday_late_late", ascending=False))

    # 9️⃣ Finally fill that new sheet in the SAME copy
    fill_schedule(df_team, df_schedule, excel, sheet_name)

    # 🔟 Return everything
    return sheet_name, start_date, end_date, full_date_range, mismatches, unrec_cells, dest

if __name__ == "__main__":

    sheet_name, start_date, end_date, full_date_range, mismatches, unrecognized_cells, output = run_schedule(
        "/home/julien/Documents/source_file.xlsx",
        "/home/julien/Documents/"
    )
    print(f"✅ File saved to: {output}")
