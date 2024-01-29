import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import numpy as np

st.title('Jellycat Daily Stock, Price, and Information')

st.markdown(
    """
    This project displays daily stock count, price, information and various analysis of Jellycat products :)
    
    Daily data will be updated around 11am EST/EDT.
""")

maintab1, maintab2, maintab3, maintab4, maintab5, maintab6 = st.tabs(["All Jellycats", "Jellycat Information",
                                                                        "Price & Stock Tracker", "Returning Jellycats", "Sold Out/New In", "Analysis"])

with maintab1:
    df = DeltaTable("./spark-warehouse/all").to_pandas()
    df["jellycatdatecreated"] = df["jellycatdatecreated"] - pd.Timedelta(hours=5)
    df["stockcount"].fillna(0, inplace=True)
    df["jellycatdatecreated"] = pd.to_datetime(df["jellycatdatecreated"]).dt.date
    most_recent_date = df['jellycatdatecreated'].max()
    st.header(f'All Jellycat information on {most_recent_date}')
    df = df[df["jellycatdatecreated"] == most_recent_date]
    min_price = int(df["price"].loc[df['price'].idxmin()])
    max_price = int(df["price"].loc[df['price'].idxmax()])
    ### filter
    filtercol1, filtercol2 = st.columns(2, gap="large")
    with filtercol1:
        price_filter = st.slider("Select price range", value = [0, max_price], min_value=min_price, max_value=max_price, step=10)
    df_jellycat_filter = df[(df["price"]>=price_filter[0]) & (df["price"]<=price_filter[1])]
    with filtercol2:
        stock_filter = st.slider("Select stock count range", value = [0, 999], min_value=0, max_value=999, step=10)
    df_jellycat_filter = df_jellycat_filter[(df_jellycat_filter["stockcount"]>=stock_filter[0]) & (df_jellycat_filter["stockcount"]<=stock_filter[1])]
    filtercol3, filtercol4 = st.columns(2, gap="large")
    with filtercol3:
        category_list = sorted(list(df["category"].unique()))
        category_list.insert(0, "All")
        category_filter = st.selectbox("Select category", category_list)
        if category_filter!="All":
            df_jellycat_filter = df_jellycat_filter[(df_jellycat_filter["category"]==category_filter)]
    with filtercol4:
        stock_list = list(df["stock"].unique())
        stock_list.insert(0, "All")
        stock_filter = st.selectbox("Select stock", stock_list)
        if stock_filter!="All":
            df_jellycat_filter = df_jellycat_filter[(df_jellycat_filter["stock"]==stock_filter)]

    st.dataframe(df_jellycat_filter[["jellycatname","size",'price','stockcount','stock',"category",'height','width']],
                use_container_width=True, hide_index=True,
                column_config={
                    "jellycatname": "Name",
                    "size": "Size",
                    "price": "Price (USD)",
                    "stockcount": "Stock Count",
                    "stock": "Stock Status",
                    "category": "Category",
                    "height": "Height",
                    "width": "Width"
                })

with maintab2:
    ### Search/Select jellycat
    df = DeltaTable("./spark-warehouse/all").to_pandas()
    df["jellycatdatecreated"] = df["jellycatdatecreated"] - pd.Timedelta(hours=5)
    df["stockcount"].fillna(0, inplace=True)
    df["jellycatdatecreated"] = pd.to_datetime(df["jellycatdatecreated"]).dt.date
    most_recent_date = df['jellycatdatecreated'].max()
    st.header(f'Jellycat Information on {most_recent_date}')
    df = df[df["jellycatdatecreated"] == most_recent_date]
    ### filter for jellycat name
    selected_jellycat = st.selectbox("Type to Search or Select your Jellycat", list(df["jellycatname"].unique()))
    df_selected_jellycat = df[(df["jellycatname"]==selected_jellycat)]
    df_selected_jellycat_display = df_selected_jellycat.reset_index()[["size",'price','stockcount','stock',"category",'height','width']]
    ### display image
    col1, col2, col3 = st.columns(3)
    with col2:
        st.image(list(df_selected_jellycat["imagelink"])[0], caption=selected_jellycat)

    st.dataframe(df_selected_jellycat_display,use_container_width=True, hide_index=True,
                column_config={
                    "size": "Size",
                    "price": "Price (USD)",
                    "stockcount": "Stock Count",
                    "stock": "Stock Status",
                    "category": "Category",
                    "height": "Height",
                    "width": "Width"
                })

with maintab3:
    ### price and stock tracker
    df = DeltaTable("./spark-warehouse/all").to_pandas()
    df["jellycatdatecreated"] = df["jellycatdatecreated"] - pd.Timedelta(hours=5)
    df["jellycatdatecreated"] = pd.to_datetime(df["jellycatdatecreated"]).dt.date
    df["stockcount"].fillna(0, inplace=True)
    ### filter for jellycat name
    selected_jellycat = st.selectbox("Type to Search or Select your Jellycat", list(df["jellycatname"].unique()))
    df_selected_jellycat = df[(df["jellycatname"]==selected_jellycat)]
    ### display image
    col1, col2, col3 = st.columns(3)
    with col2:
        st.image(list(df_selected_jellycat["imagelink"])[0], caption=selected_jellycat)
    df_selected_jellycat_display = df_selected_jellycat.reset_index()[["size",'price','stockcount','stock',"category",'height','width']]
    df_selected_jellycat_plot = df[(df["jellycatname"]==selected_jellycat)]
    size_list = [x.capitalize() for x in df_selected_jellycat_plot["size"].unique()]
    selected_size = st.selectbox("Select your size", size_list)
    df_selected_jellycat_plot = df_selected_jellycat_plot[df_selected_jellycat_plot["size"]==selected_size.upper()]
    df_selected_jellycat_plot = df_selected_jellycat_plot.sort_values("jellycatdatecreated", ascending=True)
    st.dataframe(df_selected_jellycat_plot)
    tab1, tab2 = st.tabs(["Stock Count :bear:", "Price :moneybag:"])
    with tab1:
        fig = px.line(df_selected_jellycat_plot, x="jellycatdatecreated", y="stockcount", color="size", symbol="size",
                        labels={
                        "jellycatdatecreated": "Date",
                        "price": "Stock Count"
                        },
                        title=selected_jellycat)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        fig = px.line(df_selected_jellycat_plot, x="jellycatdatecreated", y="price", color="size", symbol="size",
                        labels={
                        "jellycatdatecreated": "Date",
                        "price": "Price (USD)"
                        },
                        title=selected_jellycat)
        st.plotly_chart(fig, use_container_width=True)

with maintab4:
    ### Returning stock
    st.header(f'Returning Jellycats')
    df_out_of_stock = DeltaTable("./spark-warehouse/out-of-stock").to_pandas()
    df_out_of_stock["jellycatdatecreated"] = df_out_of_stock["jellycatdatecreated"] - pd.Timedelta(hours=5)
    returning_stock_list = sorted(df_out_of_stock["stock"].unique())
    if "Out of stock" in returning_stock_list:
        returning_stock_list.remove("Out of stock")
    selected_stock = st.selectbox('Returning options', returning_stock_list)
    df_returning_stock = df[df["stock"]==selected_stock]
    st.dataframe(df_returning_stock.reset_index()[["jellycatname","category","size",'height','width','stock']],use_container_width=True, hide_index=True)

with maintab5:
    ### Just restocked within last 3 days
    st.header(f'Jellycats restocked within last 3 days')
    df_restocked_within_3_days = DeltaTable("./spark-warehouse/restocked-within-3-days").to_pandas()
    df_restocked_within_3_days["jellycatdatecreated"] = df_restocked_within_3_days["jellycatdatecreated"] - pd.Timedelta(hours=5)
    st.dataframe(df_restocked_within_3_days.reset_index()[["jellycatname","size",'price',"category"]],use_container_width=True, hide_index=True)
    ### Just new in within last 3 days
    st.header(f'Jellycats new in within last 3 days')
    df_new_in = DeltaTable("./spark-warehouse/new-in").to_pandas()
    st.dataframe(df_new_in.reset_index()[["jellycatname","size",'price',"category"]],use_container_width=True, hide_index=True)
    ### Sold out within last 3 days
    st.header(f'Jellycats sold out within last 3 days')
    df_sold_out_within_3days = DeltaTable("./spark-warehouse/outofstock-within-3-days").to_pandas()
    st.dataframe(df_sold_out_within_3days.reset_index()[["jellycatname","size",'price',"category"]],use_container_width=True, hide_index=True)

with maintab6:
    st.header(f'Revenue by category on {most_recent_date}')
    df_agg_category = DeltaTable("./spark-warehouse/revenue-agg-category").to_pandas()
    fig = px.pie(df_agg_category, values="totalrevenue", names="category")
    st.plotly_chart(fig, use_container_width=True)
    
    st.header(f'Units sold by category on {most_recent_date}')
    fig = px.pie(df_agg_category, values="totalunitsold", names="category")
    st.plotly_chart(fig, use_container_width=True)

    st.header(f'Revenue by size on {most_recent_date}')
    df_agg_size = DeltaTable("./spark-warehouse/revenue-agg-size").to_pandas()
    fig = px.pie(df_agg_size, values="totalrevenue", names="size")
    st.plotly_chart(fig, use_container_width=True)
    
    st.header(f'Units sold by size on {most_recent_date}')
    fig = px.pie(df_agg_size, values="totalunitsold", names="size")
    st.plotly_chart(fig, use_container_width=True)