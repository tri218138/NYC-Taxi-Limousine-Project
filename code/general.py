from my_enum import table_name, question
import pandas as pd
from pyhive import hive
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def load_tokenizer_and_model():
    # Initialize the tokenizer from Hugging Face Transformers library
    tokenizer = T5Tokenizer.from_pretrained("t5-small")

    # Load the model
    model = T5ForConditionalGeneration.from_pretrained(
        "cssupport/t5-small-awesome-text-to-sql"
    )
    model = model.to(device)
    model.eval()


def generate_sql(tokenizer, model, input_prompt):
    # Tokenize the input prompt
    inputs = tokenizer(
        input_prompt, padding=True, truncation=True, return_tensors="pt"
    ).to(device)

    # Forward pass
    with torch.no_grad():
        outputs = model.generate(**inputs, max_length=512)

    # Decode the output IDs to a string (SQL query in this case)
    generated_sql = tokenizer.decode(outputs[0], skip_special_tokens=True)

    return generated_sql


def hive_connection():
    # Connect to Hive
    conn = hive.Connection(host="localhost", port=10000)

    # Create a cursor
    cursor = conn.cursor()

    return cursor, conn


def execute_commands(cursor, commands: list = None):
    # Execute the Hive query
    cursor.execute("USE nyc_taxi_limousine")
    cursor.execute(commands[0])
    results = cursor.fetchall()
    return results


def question_to_query(question):
    input_prompt = (
        "tables:\n"
        + f"""CREATE EXTERNAL TABLE {table_name} (
    VendorID INT,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE
)"""
        + "\n"
        + "query for:"
        + question
    )

    generated_sql = generate_sql(input_prompt)

    print(f"The generated SQL query is: {generated_sql}")
    return generated_sql


def visualize_result(results):
    return results


def main():
    cursor, conn = hive_connection()
    query = question_to_query(question)
    results = execute_commands(cursor, [query])
    cursor.close()
    conn.close()
    print(visualize_result(results))


if __name__ == "__main__":
    main()
