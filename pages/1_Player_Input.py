import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent))

import asyncio
import src.ems.functions.sim_progress as sim_progress
import src.ems.functions.sql_queries as sql
import src.ems.st_pages.input_dashboard as input_dashboard
import streamlit as st

#Streamlit
st.set_page_config(layout = "wide")
st.sidebar.markdown("Main Page")
st.header("Investment Dashboard", divider = "blue")

def main():
    player = 'RedCo'
    db_engine = input_dashboard.postgres_connect()
    input_dashboard.initialize_session_state(db_engine)

    selected_simulation = input_dashboard.render_simulation_selection(db_engine)
    view_round = input_dashboard.render_round_selection(db_engine, selected_simulation)

    with st.container():
        investment_selection_col, investment_input_dashboard_col = st.columns([2/5, 3/5], border = True)
        brief, market_target_tab, investment_summary_tab, investment_metric_tab,  = investment_input_dashboard_col.tabs(
            ["Brief", "Market Target", "Investment Summary", "Investment Metrics"]
        )

        with investment_selection_col:
            #selected_player = input_dashboard.render_player_selection(db_engine)
            st.header(player)
            input_dashboard.render_investment_selection(db_engine, player) 

            simulation_name_col, submit_button_col = st.columns([0.7,0.3])
            with simulation_name_col:
                simulation_name = input_dashboard.render_simulation_name_text_input(selected_simulation)
    
            with submit_button_col:
                input_dashboard.render_submit_simulation_button()
            
            if st.session_state.submit_clicked is True:
                with st.spinner("Waiting Simulation Result..."):
                    current_round = sql.get_current_round(db_engine, simulation_name)
                    sim_progress.update_asset_table(db_engine, simulation_name)
                    sim_progress.update_progress_table(db_engine, simulation_name, player)
                    asyncio.run(sim_progress.wait_for_table(db_engine, f'mart_{simulation_name}', f'reporting_sim_result_{current_round}'))
                st.session_state.submit_clicked = False

        with brief:
            st.markdown(sql.get_player_description(db_engine, player))

        with investment_summary_tab:
            input_dashboard.render_investment_summary_bar(db_engine, player, view_round, selected_simulation)
    
        with market_target_tab:
            selected_tech = st.selectbox(
                label = 'Select Technology',
                options = list(sql.get_investment_options(db_engine).keys()) + ['Coal']
            )
            input_dashboard.render_target_chart(db_engine, selected_simulation, selected_tech, view_round)


if __name__ == "__main__":
    main()