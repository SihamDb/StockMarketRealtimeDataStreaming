from random import random
from datetime import datetime
import random
import math


# Function to generate time series data with a pattern
def generate_market_data(max_event_counts, ticker):
    base_value = 100
    amplitude = 20
    frequency = 0.1  # Determines the cycle length
    noise = random.uniform(-5, 5)  # Random noise
    value = base_value + amplitude * math.sin(frequency * max_event_counts) + noise
    data = {
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "ticker": ticker,
        "price": round(value, 2),
    }
    return data
