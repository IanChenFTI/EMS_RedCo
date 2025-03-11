import src.ems.functions.sql_queries as sql
import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
import urllib

START_YEAR = int(os.getenv("START_YEAR"))
YEARS_PER_SIMULATION = int(os.getenv("YEARS_PER_SIMULATION"))

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

def initialize_session_state(db_engine):
    #initialize session state variable if does not exist already
    all_players = sql.get_all_players(db_engine)
    all_investment_tech = sql.get_investment_options(db_engine)

    for player in all_players:
        for tech in all_investment_tech.keys():
            if f'{player}+{tech}' not in st.session_state:
                st.session_state[f'{player}+{tech}'] = 0

    #One for submit button as well
    if 'submit_clicked' not in st.session_state:
        st.session_state.submit_clicked = False

def render_simulation_selection(db_engine):
    #Load player information from the initial_game_state
    selected_simulation = st.selectbox(
        "Select Simulation File",
        sql.get_all_simulation(db_engine),
        index = None,
        placeholder="Default Simulation"
    )

    return selected_simulation

def render_round_selection(db_engine, sim_name):
    #Create a slider given a game state is selected
    if sim_name is not None:
        selected_round = st.slider(
            "Select Game Round",
            min_value = 0,
            max_value = sql.get_current_round(db_engine, sim_name)
        )
    else:
        selected_round = 0

    return selected_round

def render_player_selection(db_engine):
    """
    Render the selection box to select player for their investment input
    args:
        - db_engine: postgres db sqlalchemy engine where the specs of investment options are stored
        - column: streamlit column object where the radio buttons are placed within
    """
    selected_player = st.selectbox(
        "Select Player",
        sql.get_all_players(db_engine)
    )

    return selected_player

def update_investment_selection(player, tech, value):
    st.session_state[f'{player}+{tech}'] = st.session_state[value]

def render_investment_selection(db_engine, selected_player):
    """
    Render the radio buttons for investment selection 
    args:
        - db_engine: postgres db sqlalchemy engine where the specs of investment options are stored
        - column: streamlit column object where the radio buttons are placed within
        - selected_player: player that is currently selecting for their investment
    """
    investment_max_build = sql.get_investment_options(db_engine)
    
    for tech, no_buttons in investment_max_build.items():
        capacity = sql.get_investment_capacity(db_engine, tech)
        st.radio(
            f"{tech} ({capacity} MW)",
            [x for x in range(0, no_buttons+1)],
            horizontal=True,
            index = st.session_state[f'{selected_player}+{tech}'],
            key = f'{selected_player}+{tech}_radio', 
            on_change = update_investment_selection,
            args=[selected_player, tech,  f'{selected_player}+{tech}_radio']
        )

def summarize_pending_assets(db_engine, selected_player):
    investment_options = sql.get_investment_options(db_engine)
    all_rows = []
    for tech, capacity in investment_options.items():
        key = f'{selected_player}+{tech}'
        val = st.session_state[key]
        if key.split("+")[0] == selected_player:
            additional_row = pd.DataFrame(
                data = {
                    "Asset Type": tech,
                    "Capacity": val*sql.get_investment_capacity(db_engine, tech),
                    "Established": "No",
                },
                index = [0]
            )
            all_rows.append(additional_row)
    pending_asset_df = pd.concat(all_rows)
    return pending_asset_df

def render_investment_summary_bar(db_engine, selected_player, view_round, selected_simulation):
    existing_asset_summary_df = sql.get_existing_asset_summary(db_engine, selected_player, view_round, selected_simulation)
    pending_asset_summary_df = summarize_pending_assets(db_engine, selected_player)
    df_asset_summary = pd.concat([existing_asset_summary_df, pending_asset_summary_df])
    fig = px.bar(
        df_asset_summary,
        title="MW Capacity",
        x = 'Asset Type',
        y = 'Capacity',
        color = "Established"
    )
    st.plotly_chart(fig, use_container_width = True)
    #st.table(investment_summary_df)

def render_simulation_name_text_input(selected_simulation):
    
    simulation_name = st.text_input(
        "Round Name", 
        placeholder="Name the round", 
        label_visibility="collapsed",
        value = selected_simulation,
        disabled = selected_simulation is not None #disable name change if simulation already exist
    )

    return simulation_name

def submit_clicked():
    st.session_state.submit_clicked = True

def render_submit_simulation_button():
    st.button("Submit", on_click = submit_clicked)

#async def render_submit_simulation_button(db_engine, simulation_name):
#    #sim_progress.update_asset_table(db_engine, selected_simulation, simulation_name)
#    
#    #with st.spinner("Constructing Simulation Model..."):
#    #    model_dict = Construct_SimulationModel(db_engine, simulation_name)
#    if st.button("Submit"):
#        with st.spinner("Waiting Simulation Result..."):
#            current_round = sql.get_current_round(db_engine, simulation_name)
#            await sim_progress.wait_for_table(db_engine, f'mart_{simulation_name}', f'reporting_sim_result_{current_round}')
#

def render_target_chart(db_engine, sim_name, selected_tech, view_round):

    #Plotting Target
    target_df = sql.get_target_table(db_engine)
    target_df = target_df.drop(columns = ['index'])
    target_df = target_df.transpose()
    target_df.columns = target_df.iloc[0]
    target_df = target_df.iloc[1:]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x = target_df.index,
            y = target_df[selected_tech],
            line = dict(dash = 'dash'),
            name = f"Targeted {selected_tech} Capacity (MW)"
        )
    )

    #Plotting actual investment over target
    sim_name = 'initial_game_setups' if sim_name is None else f'raw_{sim_name}'
    view_year = START_YEAR + (view_round+1)*YEARS_PER_SIMULATION
    investment_summary_df = sql.get_investment_capacity_summary_table(db_engine, sim_name, view_year)
    pivoted_summary_df = pd.pivot_table(investment_summary_df, values = 'sum', index = 'start_year', columns=['asset_type'])
    missing_years = set(range(2025, view_year, 5)) - set(pivoted_summary_df.index.values)
    empty_filler_df = pd.DataFrame(
        index = list(missing_years),
        columns = pivoted_summary_df.columns
    )
    pivoted_summary_df = pd.concat([pivoted_summary_df, empty_filler_df]).sort_index().ffill()
    pivoted_summary_df.fillna(0)

    fig.add_trace(
        go.Scatter(
            x = target_df.index,
            y = pivoted_summary_df[selected_tech] - pivoted_summary_df[selected_tech].iloc[0],
            line = dict(),
            name = f"Actual {selected_tech} Capacity (MW)"
        )
    )


    st.plotly_chart(fig, use_container_width=True)