import streamlit as st
from snowflake.snowpark.functions import col
import pandas as pd # Needed for .to_pandas()
# No BooleanType needed for this specific version of the code if not dealing with ORDER_FILLED filtering here

# --- Streamlit App Title and Info ---
st.title("Custom Smoothie Form!")
st.write("Choose the fruits you want in your Custom Smoothie!") # Original line, removed "# Add this line" comment

# --- Snowflake session and data retrieval (MOVED TO TOP) ---
# Define session early so it's available for all subsequent database operations
session = st.connection("snowflake").session()

# --- 1. Your existing selectbox ---
option = st.selectbox(
    "What is Your Favorite Fruit ?",
    ("Banana", "Strawberries", "Peaches"),
)
st.write("Your Favorite Fruit is:", option)

# --- 2. Get fruit options for display and multiselect ---
# Ensure column name is correct (FRUIT_NAME vs fruit_name)
my_dataframe = session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME"))

st.dataframe(data=my_dataframe, use_container_width=True)
st.header("Fruit Options")

# --- 3. Convert Snowpark DataFrame column to a Python list for multiselect options ---
fruit_names_list = my_dataframe.to_pandas()['FRUIT_NAME'].tolist()

# --- 4. Define the multiselect and a new text input for the name ---
ingredients = st.multiselect(
    'Choose up to 5 ingredients:',
    fruit_names_list, # Pass the Python list here!
    max_selections=5
)

# Add a text input for the name on the order
name_on_order = st.text_input("Name on Smoothie Order:")


# --- 5. Start of the IF block: Process ingredients if selected ---
if ingredients:
    # --- 5a. Convert LIST to STRING with a separator ---
    ingredients_string = '' # Initialize as empty string

    for fruit_chosen in ingredients:
        ingredients_string += fruit_chosen + ', ' # Add comma and space after each fruit

    # Remove the trailing comma and space if the string is not empty
    if ingredients_string:
        ingredients_string = ingredients_string[:-2] # Removes the last ', '

    # FIX: Corrected indentation for st.write
    st.write("--- Your Selected Ingredients (Formatted String) ---")
    st.write(ingredients_string)

    # --- 5b. Define the INSERT statement ---
    # IMPORTANT: Your 'ORDERS' table should have 'INGREDIENTS' and 'NAME_ON_ORDER' columns.
    # We confirmed NAME_ON_ORDER exists in your last DESCRIBE TABLE output.
    # Also confirmed ORDER_FILLED BOOLEAN exists. Let's include that.
    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders(INGREDIENTS, NAME_ON_ORDER, ORDER_FILLED)
    VALUES ('{ingredients_string}', '{name_on_order}', FALSE);
    """

else: # This block executes if ingredients IS empty
    st.write('Please select at least one ingredient to see your custom smoothie!')
    my_insert_stmt = "No ingredients selected, no order to place." # Placeholder for display

# --- 6. Display the SQL statement for troubleshooting ---
st.write("--- SQL Statement to be Executed (for debugging) ---")
st.write(my_insert_stmt)

# --- Add a button to actually submit the order ---
submit_order_button = st.button("Submit Smoothie Order")

if submit_order_button:
    if ingredients: # Ensure ingredients are selected before attempting insert
        session.sql(my_insert_stmt).collect() # Execute the insert statement
        st.success('Your smoothie order is placed!', icon='✅')
    else:
        st.error('Please select ingredients before submitting your order.', icon='❌')
