import src.ems.functions.sql_queries as sql
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine
import urllib

def postgres_connect():
    server = st.secrets['DB_SERVER']
    driver = st.secrets['DB_DRIVER']
    database = st.secrets['DB_DATABASE']
    username = st.secrets['DB_USERNAME']
    password = st.secrets['DB_PASSWORD']
    db_params_tmp = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    db_params = urllib.parse.quote_plus(db_params_tmp)
    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params), fast_executemany = True)

    return engine

def render_simulation_selection(db_engine):
    #Load player information from the initial_game_state
    selected_simulation = st.selectbox(
        "Select Simulation File",
        sql.get_all_simulation(db_engine),
        index = None,
        placeholder="Default Simulation"
    )

    return selected_simulation

def render_detailing_selection():
    selected_round_specificity = st.selectbox(
        "Select View",
        ['Round Specific', 'Overview']
    )
    return selected_round_specificity

def render_select_view_round(db_engine, sim_name, round_specific):
    #Create a slider given a game state is selected
    current_round = sql.get_current_round(db_engine, sim_name)
    if round_specific == 'Round Specific' and sim_name is not None and current_round >= 2:
        selected_round = st.slider(
            "Select Game Round",
            min_value = 1,
            max_value = sql.get_current_round(db_engine, sim_name)
        )
    elif current_round > 0:
        selected_round = 1
    else:
        selected_round = 0

    return selected_round

def render_profit_line(engine, sim_name):
    
    all_round = sql.get_current_round(engine, sim_name)

    all_profit_list = []
    for round in range(1, all_round + 1):
        reporting_table = sql.get_reporting_table(engine, sim_name, round)
        reporting_table['profit'] = reporting_table['annual_subsidy_revenue'] + reporting_table['annual_dispatch_revenue'] - reporting_table['annual_capital_cost'] - reporting_table['annual_vom_cost'] - reporting_table['annual_fuel_cost']
        grouped_profit = reporting_table.groupby(by = ['player'])['profit'].sum()
        grouped_profit.name = str(round)
        all_profit_list.append(grouped_profit)
    
    profit_df = pd.concat(all_profit_list, axis = 1).transpose()
    
    fig = go.Figure()
    for player in sql.get_all_players(engine):
        fig.add_trace(
            go.Scatter(
                x = profit_df.index,
                y = profit_df[player],
                name = player
            )
        )

    fig.update_layout(
        xaxis_title = 'Round',
        yaxis_title = 'Profit $'
    )

    st.plotly_chart(fig, use_container_width=True)

def render_capacity_bar (df):
    items = [
        'max_capacity',
        'actual_capacity'
    ]

    #Sum the items by their technology type:
    grouped_df = df.groupby(by = ['asset_type'])[items].sum()

    fig = go.Figure()
    for item in items: 
        fig.add_trace(
            go.Bar(
                x = grouped_df.index,
                y = grouped_df[item],
                name = item
            )
        )

    fig.update_layout(
        xaxis_title = 'Technology',
        yaxis_title = 'Capacity MW'
    )

    st.plotly_chart(fig, use_container_width=True)

def render_revenue_cost_stackedbar (df):
    items = [
        'annual_dispatch_revenue',
        'annual_subsidy_revenue',
        'annual_capital_cost', 
        'annual_vom_cost',
        'annual_fuel_cost'
    ]
    
    #Sum the items by their technology type:
    grouped_df = df.groupby(by = ['asset_type'])[items].sum()

    fig = go.Figure()
    for item in items:
        multiplier = 1 if item in ['annual_dispatch_revenue', 'annual_subsidy_revenue'] else -1

        fig.add_trace(
            go.Bar(
                x = grouped_df.index,
                y = grouped_df[item]*multiplier,
                name = item
            )
        )

    fig.update_layout(
        barmode = 'relative',
        xaxis_title = 'Technology',
        yaxis_title = '$'
    )

    st.plotly_chart(fig, use_container_width=True)

def render_dollar_per_mwh_bar(df):
    wm = lambda x: np.average(x, weights=df.loc[x.index, "summed_value_mwh"])
    #Calculate market weighted average price
    market_weighted_average_price = (df['weighted_avg_price']*df['summed_value_mwh']).sum()/df['summed_value_mwh'].sum()
    #Calculated weighted average base on asset_type
    grouped_df = df.groupby(by = ['asset_type']).agg(weighted_avg_price = ("weighted_avg_price", wm))
    
    fig = go.Figure()

    #Revenue
    fig.add_trace(go.Bar(
        x = grouped_df.index,
        y = grouped_df['weighted_avg_price'],
        name = "$/MWh"
    ))

    fig.add_hline(
        y = market_weighted_average_price, 
        line_width = 3,
        line_color = "#1f77b4",
        annotation_text = f"Market Price @ {market_weighted_average_price:.2f}",
        annotation_position = "top right"
    )

    fig.update_layout(
        xaxis_title = 'Technology',
        yaxis_title = 'Weighted Average Price $/MWh'
    )

    st.plotly_chart(fig, use_container_width=True)