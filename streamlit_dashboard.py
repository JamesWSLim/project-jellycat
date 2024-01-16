import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import matplotlib.pyplot as plt
import seaborn as sns

st.title('Jellycat Daily Stock, Price, and Information')

st.markdown(
    """
    This app performs web scraping of Jellycat stock availability and information data :)
    
    Daily data will be updated around 11am EST/EDT.
""")

### Search/Select jellycat
df = DeltaTable("./spark-warehouse/all").to_pandas()
most_recent_date = pd.to_datetime(df['jellycatdatecreated'].max())
st.header(f'Display Jellycat Stock Availability, Price, and Information on {most_recent_date.date()}')
df = df[df["jellycatdatecreated"] == most_recent_date]
### filter for jellycat name
selected_jellycat = st.selectbox("Type to Search or Select your Jellycat", list(df["jellycatname"].unique()))
df_selected_jellycat = df[(df["jellycatname"]==selected_jellycat)]
st.dataframe(df_selected_jellycat.reset_index()[["size",'price','stock',"category",'height','width']],use_container_width=True, hide_index=True)

### display image
col1, col2, col3 = st.columns(3)
with col2:
    st.image(list(df_selected_jellycat["imagelink"])[0], caption=selected_jellycat)

### price tracker
font = {'size': 9}
plt.rc('font', **font)
df_price_tracker = DeltaTable("./spark-warehouse/all").to_pandas()
selected_jellycat_df = df_price_tracker[(df_price_tracker["jellycatname"]==selected_jellycat)]
size_list = list(selected_jellycat_df["size"].unique())
palette = sns.color_palette()
fig = plt.figure(figsize=(10,5))
plt.ylim(-2, 2)
plt.xlabel("Date")
plt.ylabel("Price")
min_price = selected_jellycat_df["price"].min()
max_price = selected_jellycat_df["price"].max()
min_limit = float(min_price) - float(max_price-min_price)*0.1
max_limit = float(max_price) + float(max_price-min_price)*0.1
plt.ylim(min_limit, max_limit)
sns.set_style("ticks")
for i, size in enumerate(size_list):
    sns.lineplot(data=selected_jellycat_df[(selected_jellycat_df["size"]==size)],x="jellycatdatecreated",y='price',color=palette[i], label=size)
sns.despine(left=True)
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
st.pyplot(fig)

### Returning stock
st.header(f'Returning Jellycats')
df_out_of_stock = DeltaTable("./spark-warehouse/out-of-stock").to_pandas()
returning_stock_list = sorted(df_out_of_stock["stock"].unique())
returning_stock_list.remove("Out of stock")
selected_stock = st.selectbox('Returning options', returning_stock_list)
df_returning_stock = df[df["stock"]==selected_stock]
st.dataframe(df_returning_stock.reset_index()[["jellycatname","size",'price',"category",'height','width']],use_container_width=True, hide_index=True)

### Just restocked within last 3 days
st.header(f'Jellycats restocked within last 3 days')
df_restocked_within_3_days = DeltaTable("./spark-warehouse/restocked-within-3-days").to_pandas()
st.dataframe(df_restocked_within_3_days.reset_index()[["jellycatname","size",'price',"category"]],use_container_width=True, hide_index=True)

### Just new in within last 3 days
st.header(f'Jellycats new in within last 3 days')
df_new_in = DeltaTable("./spark-warehouse/new-in").to_pandas()
st.dataframe(df_new_in.reset_index()[["jellycatname","size",'price',"category"]],use_container_width=True, hide_index=True)

### Sold out within last 3 days
st.header(f'Jellycats sold out within last 3 days')
df_sold_out_within_3days = DeltaTable("./spark-warehouse/outofstock-within-3-days").to_pandas()
st.dataframe(df_sold_out_within_3days.reset_index()[["jellycatname","size",'price',"category"]],use_container_width=True, hide_index=True)