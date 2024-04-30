import requests
import json
import pandas as pd
import time

url = "https://graphql.bitquery.io"

# Initialize variables
limit = 25000  # Number of records to fetch per request
offset = 0    # Initial offset value

# File path to store the CSV
csv_file_path = r'tron_dex_trades_data2.csv'

# Function to fetch data and append to CSV
def fetch_and_append_data(offset):
    all_trades = []

    query = f"""
        query {{
            tron(network: tron) {{
                dexTrades(
                    date: {{between: ["2024-03-21", "2024-04-21"]}}
                    options: {{limit: {limit}, offset: {offset}, asc: "block.timestamp.time"}}
                    exchangeAddress: {{
                        in: [
                            "TKWJdrQkqHisa1X8HUdHEfREvTzw4pMAaY", 
                            "THkvXfUaRmpkRcfh5RpokxfXBN9U3eRGj7", 
                            "TBKLPydCjJejEoCHNach2z5Mcwv2q7Vwag"
                        ]
                    }}
                ) {{
                    date {{
                        date
                    }}
                    block {{
                        hash
                        timestamp {{
                            time
                        }}
                    }}
                    transaction {{
                        hash
                    }}
                    exchange {{
                        address {{
                            address
                        }}
                    }}
                    smartContract {{
                        address {{
                            address
                        }}
                    }}
                    maker {{
                        address
                    }}
                    taker {{
                        address
                    }}
                    side
                    baseAmount
                    baseCurrency {{
                        name
                        symbol
                    }}
                    buyAmount
                    buyCurrency {{
                        name
                        symbol
                    }}
                    sellAmount
                    sellCurrency {{
                        name
                        symbol
                    }}
                    quoteAmount
                    quoteCurrency {{
                        name
                        symbol
                    }}
                    price
                    quotePrice
                    energyFee
                    energyUsageTotal
                    fee
                    tradeAmount(in: USD)
                }}
            }}
        }}
        """

    # Construct the payload with the current offset
    payload = {
        "query": query,
        "variables": "{}"
    }

    # Convert payload to JSON format
    payload_json = json.dumps(payload)

    # Set headers
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': 'BQYsiXQpLlrG1tmEav6glGMuPgceuPPZ',
        'Authorization': 'Bearer ory_at_BIR1LJNp4j1ufJZROSvfaQR8gvIXWNEYC-hxkChTHkc.60Ty-ppn3stu-lU2hg8siMr_0j-eElkUFJn9Jrl5yWY'
    }

    # Define initial retry delay and maximum retries
    retry_delay = 1
    max_retries = 5
    retries = 0

    while retries < max_retries:
        # Make request
        response = requests.post(url, headers=headers, data=payload_json)

        # Check if request was successful
        if response.status_code == 200:
            # Parse JSON response
            data = response.json()

            # Extract relevant data
            trades = data['data']['tron']['dexTrades']

            # Append trades to all_trades list
            all_trades.extend(trades)

            # Create DataFrame from all_trades
            df = pd.DataFrame(all_trades)

            # Append DataFrame to CSV file
            df.to_csv(csv_file_path, mode='a', header=not offset, index=False)  # Append mode, don't write headers if not the first batch

            # Print message after data is appended
            print(len(all_trades), "Records appended successfully.")
            
            return True, all_trades

        elif response.status_code == 503:
            print("503 Error: Service Unavailable. Retrying in", retry_delay, "seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
            retries += 1
        else:
            print("Error occurred while fetching data. Status code:", response.status_code)
            return False, []

    print("Max retries exceeded. Data fetching failed.")
    return False, []

# Fetch and append data while there are more records
while True:
    # Fetch and append data
    success, fetched_trades = fetch_and_append_data(offset)

    # Break the loop if data fetching fails
    if not success:
        print("Data fetching failed.")
        break

    # If the number of records fetched is less than the limit, all data has been collected
    if len(fetched_trades) < limit:
        print("All data collected.")
        break

    # Increment offset
    offset += limit

    # Sleep to comply with rate limits (10 API calls per minute)
    time.sleep(6)  # Sleep for 6 seconds (60 / 10 = 6)


