from flask import Flask, request, jsonify, render_template
import os
import pandas as pd
from datetime import datetime
from generate_data import generate_date_list, fetch_and_merge_data  # Import your helper functions

# Create a Flask app instance
app = Flask(__name__)

# Define a route for the home page
@app.route('/')
def home():
    return render_template("index.html")

@app.route('/data', methods=['GET'])
def get_data():
    # Get query parameters from the URL
    country = request.args.get('country', None)  # Optional country filter
    data_type = request.args.get('type', 'monthly')  # 'monthly' or 'weekly', default to 'monthly'

    # Validate the type
    if data_type not in ['monthly', 'weekly']:
        return {"error": "Invalid type. Please use 'monthly' or 'weekly'."}, 400

    # Generate date list (default range from July 2023 to now)
    date_list = generate_date_list()

    # Fetch the merged data
    try:
        master_data = fetch_and_merge_data(date_list, data_type=data_type)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Filter by country if provided
    if country:
        master_data = master_data[master_data["country_code"] == country.upper()]

    # Return data as JSON
    return master_data.to_json(orient="records")


if __name__ == '__main__':
    app.run(debug=True)