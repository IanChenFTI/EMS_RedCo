import src.ems.st_pages.result_dashboard as dashboard
import src.ems.functions.sql_queries as sql
import streamlit as st

#Streamlit
st.set_page_config(layout = "wide")
st.sidebar.markdown("Main Page")
st.header("Simulation Result Dashboard", divider = "blue")

def main():
    pg_engine = dashboard.postgres_connect()

    view_simulation_col, view_round_col, view_player_col = st.columns([1/3, 1/3, 1/3])

    with view_simulation_col:
        selected_simulation = dashboard.render_simulation_selection(pg_engine)

    with view_round_col:
        selected_round_specific = dashboard.render_detailing_selection()
    
    selected_round = dashboard.render_select_view_round(pg_engine, selected_simulation, selected_round_specific)


    #Dashboard Components, only available if valid simulation selected
    dashboard_left, dashboard_right = st.columns([0.5, 0.5])
    if selected_round_specific == 'Round Specific' and selected_simulation is not None:
        reporting_table = sql.get_reporting_table(pg_engine, selected_simulation, selected_round)
        with dashboard_left:
            dashboard.render_dollar_per_mwh_bar(reporting_table)
            dashboard.render_capacity_bar(reporting_table)
        with dashboard_right:
            dashboard.render_revenue_cost_stackedbar(reporting_table)

    elif selected_round_specific == 'Overview' and selected_simulation is not None:
        with dashboard_left:
            dashboard.render_profit_line(pg_engine, selected_simulation)
        #with dashboard_right:


if __name__ == "__main__":
    main()