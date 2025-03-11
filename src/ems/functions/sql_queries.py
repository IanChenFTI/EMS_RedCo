import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
import traceback

START_YEAR = int(st.secrets["START_YEAR"])
YEARS_PER_SIMULATION = int(st.secrets["YEARS_PER_SIMULATION"])

def create_storage_schemas(engine, sim_name) -> None:

    with engine.connect() as con:
        #add primary key to player_table
        con.execute(text(f'create schema raw_{sim_name}'))
        con.execute(text(f'create schema stage_{sim_name}'))
        con.execute(text(f'create schema mart_{sim_name}'))
        #replicate an asset table since it is simulation unique
        con.execute(text(f'select * into raw_{sim_name}.asset_table from initial_game_setups.asset_table'))
        #create a table that tracks player input
        con.execute(text(f'create table raw_{sim_name}.input_progress(player text,round int)'))
        try:
            con.commit()
        except ProgrammingError as e:
            print(f'{sim_name} already exists!')
            traceback.print_exc()


def get_all_simulation(engine):

    query = text(
        """
        select 
            RIGHT(name, CHARINDEX('_', REVERSE(name) + '_') - 1) as all_simulation
        from sys.schemas 
        where name like 'mart_%'
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return list(table.all_simulation)

def get_all_players(engine):

    query = text(
        """
        select 
            distinct(player) as players
        from initial_game_setups.player_table
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return list(table.players)

def get_asset_life_profile(engine):
    query = text(
        """
        select 
            *
        from initial_game_setups.asset_life_table
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return table

def get_capital_cost_profile(engine):
    query = text(
        """
        select 
            *
        from initial_game_setups.capital_cost_table
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return table

def get_asset_table(engine, sim_name):
    query = text(
        f"""
        select 
            *
        from raw_{sim_name}.asset_table
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return table


def get_current_round(engine, sim_name):

    query = text(
        f"""
        with all_sim_gen_tables as (
            select
                t.name as table_name,
                s.name as schema_name
            from sys.tables t
            left join sys.schemas s
            on s.schema_id = t.schema_id
        )
        select 
            count(*) as number_of_rounds
        from 
            all_sim_gen_tables
        where 
            schema_name = 'raw_{sim_name}' and
            table_name like 'simulation_result%'
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return table.number_of_rounds[0]
    
def get_investment_options(engine):
    query = text(
        f"""
        select
            asset_type,
            max_build
        from initial_game_setups.investment_table
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)
    
    investment_dict = pd.Series(table.max_build.values, index = table.asset_type).to_dict()
    return investment_dict
    
def get_investment_spec(engine, technology):
    tech_cap_col = 'storage_capacity' if technology == 'Battery' else 'generation_capacity'
    query = text(
        f"""
        select
            *
        from initial_game_setups.investment_table
        where asset_type = '{technology}'
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)
    
    return table

def get_investment_capacity(engine, technology):
    tech_cap_col = 'storage_capacity' if technology == 'Battery' else 'generation_capacity'
    query = text(
        f"""
        select
            {tech_cap_col} as capacity
        from initial_game_setups.investment_table
        where asset_type = '{technology}'
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)
    
    return table.capacity[0]

def get_player_description(engine, player):
    query = text(
        f"""
        select
            description
        from initial_game_setups.player_table
        where player = '{player}'
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)
    
    return table.description[0]

def get_existing_asset_summary(engine, player, view_round, selected_sim):
    view_year = START_YEAR + (view_round)*YEARS_PER_SIMULATION
    schema = 'initial_game_setups' if selected_sim is None else f'raw_{selected_sim}'
    query = text(
        f"""
        with generation_capacity as(
            select
                asset_type as "Asset Type",
                sum(generation_capacity) as "Capacity", 
                'Yes' as "Established"
            from {schema}.asset_table
            where 
                player = '{player}' and 
                asset_type != 'Battery' and
                start_year <= {view_year}
            group by asset_type
        ), storage_capacity as (
            select
                asset_type as "Asset Type",
                sum(storage_capacity) as "Capacity", 
                'Yes' as "Established"
            from {schema}.asset_table
            where
                player = '{player}' and 
                asset_type != 'Battery' and
                start_year <= {view_year}
            group by asset_type
        )

        select *
        from generation_capacity
        union
        select *
        from storage_capacity
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)
    
    return table

def get_sim_input_capacity(engine, sim_name, current_year, generation = True):
    capacity_column = 'generation_capacity' if generation else 'storage_capacity'
    type_condition = "!=" if generation else "="

    query = text(
        f"""
        select
            asset_name,
            {capacity_column} as capacity,
            vom,
            fuel_cost,
            end_year
        from raw_{sim_name}.asset_table
        where asset_type {type_condition} 'Battery'
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    table = table[table['end_year'] >= current_year]
    capacity_dict_raw = pd.Series(table.capacity.values, index = table.asset_name).to_dict()
    capacity_dict = {}
    for player in get_all_players(engine):
        player_specific_dict = {key: value for key, value in capacity_dict_raw.items() if key.startswith(player)}
        capacity_dict = capacity_dict | {player: player_specific_dict}
    vom_dict = pd.Series(table.vom.values, index = table.asset_name).to_dict()
    fuel_cost_dict = pd.Series(table.fuel_cost.values, index = table.asset_name).to_dict()

    return capacity_dict, vom_dict, fuel_cost_dict

def get_sim_input_profiles(engine, profile_type, sample_year):
    
    if profile_type not in ['demand', 'solar', 'wind']:
        raise ValueError("Profile are only available in 'demand', 'solar' or 'wind'")
    
    query = text(
        f"""
        select
            value
        from initial_game_setups.{profile_type}_profile
        where year = {sample_year}
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    profile_dict = pd.Series(table.value.values, index = np.arange(1, len(table)+1)).to_dict()
    return profile_dict

def get_reporting_table(engine, sim_name, round):
    
    query = text(
        f"""
        select *
        from mart_{sim_name}.reporting_sim_result_{round}
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return table

def get_target_table(engine):
    query = text(
        f"""
        select *
        from initial_game_setups.target_table
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return table

def get_investment_capacity_summary_table(engine, sim_name, view_year):
    query = text(
        f"""
        with asset_summary as (
                select
                    asset_type,
                    start_year,
                    sum(
                        case
                            when asset_type = 'Battery' then storage_capacity
                            when asset_type != 'Battery' then generation_capacity
                        end
                    ) as summed_capacity
                from {sim_name}.asset_table
                where 
                    start_year < {view_year}
                group by asset_type, start_year
            )
            select
                asset_type,
                start_year,
                sum(summed_capacity) over (partition by asset_type order by start_year) as sum
            from asset_summary
            order by asset_type, start_year
        """
    )

    with engine.connect() as con:
        table = pd.read_sql(sql = query, con = con)

    return table