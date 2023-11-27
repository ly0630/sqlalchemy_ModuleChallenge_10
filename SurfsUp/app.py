import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

# Database Setup
engine = create_engine("sqlite:///hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save reference to the table
measurement = Base.classes.measurement
station = Base.classes.station

# Flask Setup
app = Flask(__name__)

# Constants
START_DATE = '2016-08-23'

# Helper function to execute queries
def execute_query(session, *args, **kwargs):
    return session.query(*args).filter(**kwargs).all()

# Flask Routes

@app.route("/")
def welcome():
    """List all available API routes."""
    return (
        f"Available Routes for Hawaii Weather Data:<br/><br>"
        f"-- Daily Precipitation Totals for Last Year: <a href=\"/api/v1.0/precipitation\">/api/v1.0/precipitation<a><br/>"
        f"-- Active Weather Stations: <a href=\"/api/v1.0/stations\">/api/v1.0/stations<a><br/>"
        f"-- Daily Temperature Observations for Station USC00519281 for Last Year: <a href=\"/api/v1.0/tobs\">/api/v1.0/tobs<a><br/>"
        f"-- Min, Average & Max Temperatures for Date Range: /api/v1.0/trip/yyyy-mm-dd/yyyy-mm-dd<br>"
        f"NOTE: If no end-date is provided, the trip API calculates stats through 08/23/17<br>" 
    )

# Precipitation query function
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create session
    with Session(engine) as session:
        sel = [measurement.date, func.sum(measurement.prcp)]
        precipitation = execute_query(session, *sel, date>=START_DATE).group_by(measurement.date).order_by(measurement.date)
        
    # Create dictionary with the date as key and the daily precipitation total as value
    precipitation_dict = {date: dailytotal for date, dailytotal in precipitation}

    # Return JSON for API query
    return jsonify(precipitation_dict)

# Stations query function
@app.route("/api/v1.0/stations")
def stations():
    # Create session
    with Session(engine) as session:
        sel = [measurement.station]
        active_stations = execute_query(session, *sel).group_by(measurement.station)
    
    # Convert list of tuples into a normal list and return JSON for the query
    list_of_stations = list(np.ravel(active_stations)) 
    return jsonify(list_of_stations)

# Station temperatures query function
@app.route("/api/v1.0/tobs")
def tobs():
    # Create session (link)
    with Session(engine) as session:
        sel = [measurement.date, measurement.tobs]
        station_temps = execute_query(session, *sel, date>=START_DATE, measurement.station=='USC00519281').group_by(measurement.date).order_by(measurement.date)
    
    # Create dictionary with the date as key and the daily temperature observation as value
    most_active_tobs_dict = {date: observation for date, observation in station_temps}

    # Return JSON for API query
    return jsonify(most_active_tobs_dict)

# Function to calculate the min/max/avg temperatures in the last 12 months
def calculate_trip(start_date, end_date='2017-08-23'):
    # Open session
    with Session(engine) as session:
        query_result = execute_query(session, func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs), date>=start_date, date<=end_date)
    
    # Create dictionary in list
    trip_stats = [{"Min": min_temp, "Average": avg_temp, "Max": max_temp} for min_temp, avg_temp, max_temp in query_result]
    return trip_stats

# Function to return the calculated trip
@app.route("/api/v1.0/trip/<start_date>/<end_date>")
def combined_trip(start_date, end_date='2017-8-23'):
    trip_stats = calculate_trip(start_date, end_date)

    if trip_stats:
        return jsonify(trip_stats)
    else:
        return jsonify({"error": "Invalid date range or dates not formatted as YYYY-MM-DD."}), 404

# Debug
if __name__ == '__main__':
    app.run(debug=True)
