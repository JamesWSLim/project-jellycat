import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import matplotlib.pyplot as plt
import seaborn as sns
import numpy
import datetime
from PIL import Image

st.title('Jellycat Daily Stock, Price, and Information')

st.markdown(
    """
    This app performs web scraping of Jellycat stock availability and information data :)
""")

### import data and convert deltatable to dataframe
df = DeltaTable("./spark-warehouse/all").to_pandas()
most_recent_date = pd.to_datetime(df['jellycatdatecreated'].max())
df = df[df["jellycatdatecreated"] == most_recent_date]

### filter for jellycat name
selected_jellycat = st.selectbox("Type to Search or Select your Jellycat", list(df["jellycatname"].unique()))

df_selected_jellycat = df[(df["jellycatname"]==selected_jellycat)]

### display image
col1, col2, col3 = st.columns(3)
with col2:
    st.image(list(df_selected_jellycat["imagelink"])[0], caption=selected_jellycat)

st.header(f'Display Jellycat Stock Availability, Price, and Information on {most_recent_date.date()}')
st.dataframe(df_selected_jellycat.reset_index()[["size",'price','stock',"category",'height','width']])

### Returning stock
st.header(f'Returning Jellycats')
df_out_of_stock = DeltaTable("./spark-warehouse/out-of-stock").to_pandas()
returning_stock_list = sorted(df_out_of_stock["stock"].unique())
returning_stock_list.remove("Out of stock")
selected_stock = st.selectbox('Returning options', returning_stock_list)
df_returning_stock = df[df["stock"]==selected_stock]
st.dataframe(df_returning_stock.reset_index()[["jellycatname","size",'price',"category",'height','width']])