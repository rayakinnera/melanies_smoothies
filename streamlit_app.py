import streamlit as st
from snowflake.snowpark.functions import col, when_matched # Combined col and when_matched imports
from snowflake.snowpark.context import get_active_session
import pandas as pd
from snowflake.snowpark.types import BooleanType
streamlit.title("My Parents new Healthy Diner")
st.write("Choose the fruits you want in your Custom Smoothie!") 

session = st.connection("snowflake").session()
option = st.selectbox(
    "What is Your Favorite Fruit ?",
    ("Banana", "Strawberries", "Peaches"),
)
st.write("Your Favorite Fruit is:", option)

my_dataframe = session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME"))

st.dataframe(data=my_dataframe, use_container_width=True)
st.header("Fruit Options")

fruit_names_list = my_dataframe.to_pandas()['FRUIT_NAME'].tolist()

ingredients = st.multiselect(
    'Choose up to 5 ingredients:',
    fruit_names_list, # Pass the Python list here!
    max_selections=5
)

name_on_order = st.text_input("Name on Smoothie Order:")

if ingredients:
    ingredients_string = '' # Initialize as empty string

    for fruit_chosen in ingredients:
        ingredients_string += fruit_chosen + ', ' # Add comma and space after each fruit
    if ingredients_string:
        ingredients_string = ingredients_string[:-2] # Removes the last ', '

    st.write("--- Your Selected Ingredients (Formatted String) ---")
    st.write(ingredients_string)


    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders(INGREDIENTS, NAME_ON_ORDER, ORDER_FILLED)
    VALUES ('{ingredients_string}', '{name_on_order}', FALSE);
    """

else: # This block executes if ingredients IS empty
    st.write('Please select at least one ingredient to see your custom smoothie!')
    my_insert_stmt = "No ingredients selected, no order to place." # Placeholder for display
st.write("--- SQL Statement to be Executed (for debugging) ---")
st.write(my_insert_stmt)

submit_order_button = st.button("Submit Smoothie Order")

if submit_order_button:
    if ingredients: # Ensure ingredients are selected before attempting insert
        session.sql(my_insert_stmt).collect() # Execute the insert statement
        st.success('Your smoothie order is placed!', icon='✅')
    else:
        st.error('Please select ingredients before submitting your order.', icon='❌')
