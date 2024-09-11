
import pandas as pd
import numpy as np
import datetime as dt
import users
import metrics

default_daterange = [dt.datetime(2021, 1, 1).date(), dt.date.today()]

def get_totals_by_metric(df_cr_users, df_cr_app_launch,
    daterange=default_daterange,
    countries_list=[],
    stat="LR",
    cr_app_versions="All",
    app="Both",
    language="All",
):

    # if no list passed in then get the full list
    if len(countries_list) == 0:
        countries_list = users.get_country_list()

    df_user_list = filter_user_data(df_cr_users,  df_cr_app_launch,
        default_daterange, countries_list, stat, cr_app_versions, app=app, language=language
    )

    if stat not in ["DC", "TS", "SL", "PC", "LA"]:
        return len(df_user_list) #All LR or FO
    else:
        download_completed_count = len(
            df_user_list[df_user_list["furthest_event"] == "download_completed"]
        )

        tapped_start_count = len(
            df_user_list[df_user_list["furthest_event"] == "tapped_start"]
        )
        selected_level_count = len(
            df_user_list[df_user_list["furthest_event"] == "selected_level"]
        )
        puzzle_completed_count = len(
            df_user_list[df_user_list["furthest_event"] == "puzzle_completed"]
        )
        level_completed_count = len(
            df_user_list[df_user_list["furthest_event"] == "level_completed"]
        )

        if stat == "DC":
            return (
                download_completed_count
                + tapped_start_count
                + selected_level_count
                + puzzle_completed_count
                + level_completed_count
            )

        if stat == "TS":
            return (
                tapped_start_count
                + selected_level_count
                + puzzle_completed_count
                + level_completed_count
            )

        if stat == "SL":  # all PC and SL users implicitly imply those events
            return tapped_start_count + puzzle_completed_count + level_completed_count

        if stat == "PC":
            return puzzle_completed_count + level_completed_count

        if stat == "LA":
            return level_completed_count


# Takes the complete user lists and filters based on input data, and returns
# a new filtered dataset

def filter_user_data(df_cr_users,  df_cr_app_launch,
    daterange=default_daterange,
    countries_list=["All"],
    stat="LR",
    cr_app_versions="All",
    app="Both",
    language=["All"],
):

    # Select the appropriate dataframe based on app and stat

    if app == "CR" and stat == "LR":
        df = df_cr_app_launch
    else:
        df = df_cr_users

    # Initialize a boolean mask
    mask = (df['first_open'] >= daterange[0]) & (df['first_open'] <= daterange[1])

    # Apply country filter if not "All"
    if countries_list[0] != "All":
        mask &= df['country'].isin(set(countries_list))

    # Apply language filter if not "All" and stat is not "FO"
    if language[0] != "All" and stat != "FO":
        mask &= df['app_language'].isin(set(language))

    # Apply stat-specific filters
    if stat == "LA":
        mask &= (df['max_user_level'] >= 1)
    elif stat == "RA":
        mask &= (df['max_user_level'] >= 25)
    elif stat == "GC":  # Game completed
        mask &= (df['max_user_level'] >= 1) & (df['gpc'] >= 90)
    elif stat == "LR" or stat == "FO":
        # No additional filters for these stats beyond daterange and optional countries/language
        pass
    
    # Filter the dataframe with the combined mask
    df = df.loc[mask]

    return df


def weeks_since(daterange):
    current_date = dt.datetime.now()
    daterange_datetime = dt.datetime.combine(daterange[0], dt.datetime.min.time())

    difference = current_date - daterange_datetime

    return difference.days // 7


def build_funnel_dataframe( df_cr_users, df_cr_app_launch,
    index_col="language",
    daterange=default_daterange,
    languages=["All"],
    countries_list=["All"],
):
    df = pd.DataFrame(columns=[index_col, "LR", "DC", "TS", "SL", "PC", "RA", "LA"])
    if index_col == "start_date":
        weeks = metrics.weeks_since(daterange)
        iteration = range(1, weeks + 1)
    elif index_col == "language":
        iteration = languages

    results = []

    for i in iteration:
        if index_col == "language":
            language = [i]
        else:
            language = languages
            end_date = dt.datetime.now().date()
            start_date = dt.datetime.now().date() - dt.timedelta(i * 7)
            daterange = [start_date, end_date]

        DC = metrics.get_totals_by_metric(df_cr_users, df_cr_app_launch,
            daterange,
            stat="DC",
            language=language,
            countries_list=countries_list,
            app="CR",
        )
        SL = metrics.get_totals_by_metric(df_cr_users, df_cr_app_launch,
            daterange,
            stat="SL",
            language=language,
            countries_list=countries_list,
            app="CR",
        )
        TS = metrics.get_totals_by_metric(df_cr_users, df_cr_app_launch,
            daterange,
            stat="TS",
            language=language,
            countries_list=countries_list,
            app="CR",
        )

        PC = metrics.get_totals_by_metric(df_cr_users,  df_cr_app_launch,
            daterange,
            stat="PC",
            language=language,
            countries_list=countries_list,
            app="CR",
        )
        LA = metrics.get_totals_by_metric(df_cr_users,  df_cr_app_launch,
            daterange,
            stat="LA",
            language=language,
            countries_list=countries_list,
            app="CR",
        )
        LR = metrics.get_totals_by_metric(df_cr_users,  df_cr_app_launch,
            daterange,
            stat="LR",
            language=language,
            countries_list=countries_list,
            app="CR",
        )        
        RA = metrics.get_totals_by_metric(df_cr_users, df_cr_app_launch,
            daterange,
            stat="RA",
            language=language,
            countries_list=countries_list,
            app="CR",
        )
        GC = metrics.get_totals_by_metric(df_cr_users,  df_cr_app_launch,
            daterange,
            stat="GC",
            language=language,
            countries_list=countries_list,
            app="CR",
        )

        entry = {
            "date": dt.datetime.today(),
            "LR": LR,
            "DC": DC,
            "TS": TS,
            "SL": SL,
            "PC": PC,
            "LA": LA,
            "RA": RA,
            "GC": GC,
        }

        if index_col == "language":
            entry["language"] = language[0]
        else:
            entry["start_date"] = start_date

        results.append(entry)

    df = pd.DataFrame(results)

  #  df['date']= pd.to_datetime(df['date'], errors='coerce')

    return df

def add_level_percents(df):

    try:
        df["DC over LR"] = np.where(df["LR"] == 0, 0, (df["DC"] / df["LR"]) * 100)
        df["DC over LR"] = df["DC over LR"].astype(int)
    except ZeroDivisionError:
        df["DC over LR"] = 0

    try:
        df["TS over LR"] = np.where(df["LR"] == 0, 0, (df["TS"] / df["LR"]) * 100)
        df["TS over LR"] = df["TS over LR"].astype(int)
    except ZeroDivisionError:
        df["TS over LR"] = 0

    try:
        df["TS over DC"] = np.where(df["DC"] == 0, 0, (df["TS"] / df["DC"]) * 100)
        df["TS over DC"] = df["TS over DC"].astype(int)
    except ZeroDivisionError:
        df["TS over DC"] = 0

    try:
        df["SL over LR"] = np.where(df["LR"] == 0, 0, (df["SL"] / df["LR"]) * 100)
        df["SL over LR"] = df["SL over LR"].astype(int)
    except ZeroDivisionError:
        df["SL over LR"] = 0

    try:
        df["SL over TS"] = np.where(df["TS"] == 0, 0, (df["SL"] / df["TS"]) * 100)
        df["SL over TS"] = df["SL over TS"].astype(int)
    except ZeroDivisionError:
        df["SL over TS"] = 0

    try:
        df["PC over LR"] = np.where(df["LR"] == 0, 0, (df["PC"] / df["LR"]) * 100)
        df["PC over LR"] = df["PC over LR"].astype(int)
    except ZeroDivisionError:
        df["PC over LR"] = 0

    try:
        df["RA over LR"] = np.where(df["LR"] == 0, 0, (df["RA"] / df["LR"]) * 100)
        df["RA over LR"] = df["RA over LR"].astype(int)
    except ZeroDivisionError:
        df["RA over LR"] = 0
    try:
        df["RA over LA"] = np.where(df["LA"] == 0, 0, (df["RA"] / df["LA"]) * 100)
        df["RA over LA"] = df["RA over LA"].astype(int)
    except ZeroDivisionError:
        df["RA over LA"] = 0

    try:
        df["PC over SL"] = np.where(df["SL"] == 0, 0, (df["PC"] / df["SL"]) * 100)
        df["PC over SL"] = df["PC over SL"].astype(int)
    except ZeroDivisionError:
        df["PC over SL"] = 0

    try:
        df["LA over LR"] = np.where(df["LR"] == 0, 0, (df["LA"] / df["LR"]) * 100)
        df["LA over LR"] = df["LA over LR"].astype(int)
    except ZeroDivisionError:
        df["LA over LR"] = 0

    try:
        df["LA over PC"] = np.where(df["PC"] == 0, 0, (df["LA"] / df["PC"]) * 100)
        df["LA over PC"] = df["LA over PC"].astype(int)
    except ZeroDivisionError:
        df["LA over PC"] = 0

    try:
        df["GC over LR"] = np.where(df["LR"] == 0, 0, (df["GC"] / df["LR"]) * 100)
        df["GC over LR"] = df["GC over LR"].astype(int)
    except ZeroDivisionError:
        df["GC over LR"] = 0

    try:
        df["GC over RA"] = np.where(df["RA"] == 0, 0, (df["GC"] / df["RA"]) * 100)
        df["GC over RA"] = df["GC over RA"].astype(int)
    except ZeroDivisionError:
        df["GC over RA"] = 0
        
    return df