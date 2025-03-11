import asyncio
import src.ems.functions.sql_queries as sql
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.ext.asyncio import create_async_engine

START_YEAR = int(os.getenv("START_YEAR"))
YEARS_PER_SIMULATION = int(os.getenv("YEARS_PER_SIMULATION"))

def round_to_year(round):
    current_year = START_YEAR + (round+1)*YEARS_PER_SIMULATION
    return current_year

def initialize_simulation_schema(db_engine, sim_name):
    
    #check if name is valid
    if sim_name in sql.get_all_simulation(db_engine):
        st.error("Simulation name already exist")
        return
    if sim_name is None:
        st.error("Please enter a simulation name")
        return
    
    sql.create_storage_schemas(db_engine, sim_name)

def initialize_new_investment(db_engine, investment_list, current_round):
    """
    feeds relevant details from initial profile:
        - Capital Cost
        - Asset Life Span
    """
    new_investment_df = pd.concat(investment_list)
    capital_cost_table = sql.get_capital_cost_profile(db_engine)[['asset_type', f'{current_round}']]
    capital_cost_table.columns = ['asset_type', 'capital_cost']

    #merge capital cost
    new_investment_df = new_investment_df.merge(
        capital_cost_table, 
        how = 'left',
        left_on='asset_type',
        right_on = 'asset_type',
    )

    new_investment_df['start_year'] = round_to_year(current_round)
    new_investment_df['end_year'] = new_investment_df['start_year'] + new_investment_df['life_span']
    new_investment_df = new_investment_df.drop(columns = ['life_span'])

    return new_investment_df

def update_asset_table(db_engine, sim_name):

    new_investment_list = []
    current_round = sql.get_current_round(db_engine, sim_name)
    for player in sql.get_all_players(db_engine):
        for tech in sql.get_investment_options(db_engine).keys():
            #For each selected investment, register their details
            invested_qty = st.session_state[f'{player}+{tech}']
            for n in range(1, invested_qty + 1):
                investment_row = sql.get_investment_spec(db_engine, tech)
                investment_row = investment_row.drop(columns = ['max_build'])
                investment_row['player'] = player
                investment_row['asset_name'] = f'{player}_{round_to_year(current_round)}_{tech}_{chr(64+n)}'
                new_investment_list.append(investment_row)

    new_investment_df = initialize_new_investment(db_engine, new_investment_list, current_round)
    existing_asset_table = sql.get_asset_table(db_engine, sim_name)

    updated_asset_table = pd.concat([new_investment_df, existing_asset_table])
    updated_asset_table = updated_asset_table.drop(columns = ['index'])
    updated_asset_table.to_sql(
        name = 'asset_table',
        con = db_engine,
        schema = f'raw_{sim_name}',
        if_exists = 'append'
    )

def update_progress_table(db_engine, sim_name, player):
    current_round = sql.get_current_round(db_engine, sim_name)
    updated_progress = pd.DataFrame(
        data = {
            'player': [player],
            'round': [current_round]
        }
    )
    updated_progress.to_sql(
        name = 'input_progress',
        con = db_engine,
        schema = f'raw_{sim_name}',
        if_exists = 'append',
        index = False
    )

async def wait_for_table(engine, schema_name: str, table_name: str, poll_interval: int = 5):
    """
    Waits for a specific table to exist in the database.

    :param schema_name: The schema where the table is expected to exist.
    :param table_name: The name of the table to wait for.
    :param poll_interval: How often (in seconds) to check for the table.
    """
    while True:
        with engine.connect() as conn:  # Async connection
            query = text(f"""
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = :schema_name
                AND TABLE_NAME = :table_name
            """)
            result = conn.execute(query, {"schema_name": schema_name, "table_name": table_name})
            exists = result.scalar()
            if exists:
                print(f"Table {schema_name}.{table_name} exists!")
                return
            else:
                print(f"Table {schema_name}.{table_name} not found. Retrying in {poll_interval} seconds...")
        
        await asyncio.sleep(poll_interval)

async def wait_for_player_input(engine, sim_name: str, player: str, poll_interval: int = 5):
    """
    Waits for all player inputs
    """
    current_round = sql.get_current_round(engine, sim_name)
    while True:
        with engine.connect() as conn:  #sync connection
            query = text(f"""
                SELECT count(*)
                FROM raw_{sim_name}.input_progress
                WHERE 
                    player = '{player}' AND 
                    round = {current_round}
            """)
            result = conn.execute(query)
            exists = result.scalar()
            if exists:
                print(f"{player} has inputted for {sim_name} round {current_round}")
                return
            else:
                print(f"{player}'s input for {sim_name} round {current_round} not found. Retrying in {poll_interval} seconds...")
        
        await asyncio.sleep(poll_interval)