import time
import random
import json

class DakarRallySimulator:

    def _decimal_to_dms(self, coord):
        """Converts decimal coordinates to degrees, minutes, seconds."""
        degrees = int(coord)
        minutes = int((coord - degrees) * 60)
        seconds = round((coord - degrees - minutes / 60) * 3600, 2)
        return f"{degrees}°{minutes}'{seconds}\""

    def simulate_vehicle_performance(self):
        """Simulates vehicle performance data with multiple sensors per category."""

        # Define sensor ranges and noise levels
        engine_rpm_range = (1000, 8000)
        engine_temp_range = (70, 110)  # Degrees Celsius
        fuel_level_range = (0, 100)  # Percentage
        oil_pressure_range = (20, 80)  # PSI
        tire_pressure_range = (25, 35)  # PSI
        transmission_temp_range = (70, 120)  # Degrees Celsius
        suspension_travel_range = (0, 10)  # Centimeters

        sensor_noise = 0.05  # Standard deviation for sensor noise

        vehicle_data = {
            "engine_rpm": [
                    random.uniform(engine_rpm_range[0], engine_rpm_range[1]) + random.gauss(0, sensor_noise) for _ in range(3)
                ],
                "engine_temp": [
                    random.uniform(engine_temp_range[0], engine_temp_range[1]) + random.gauss(0, sensor_noise) for _ in range(3)
                ],
                "fuel_level": [
                    random.uniform(fuel_level_range[0], fuel_level_range[1]) + random.gauss(0, sensor_noise) for _ in range(3)
                ],
                "oil_pressure": [
                    random.uniform(oil_pressure_range[0], oil_pressure_range[1]) + random.gauss(0, sensor_noise) for _ in range(3)
                ],
                "tire_pressure": {
                    "FR":[
                        random.uniform(tire_pressure_range[0], tire_pressure_range[1]) + random.gauss(0, sensor_noise) 
                        for _ in range(3)
                        ],
                    "FL": [
                        random.uniform(tire_pressure_range[0], tire_pressure_range[1]) + random.gauss(0, sensor_noise) 
                        for _ in range(3)
                        ],
                    "RR": [
                        random.uniform(tire_pressure_range[0], tire_pressure_range[1]) + random.gauss(0, sensor_noise) 
                        for _ in range(3)
                        ],
                    "RL":[
                        random.uniform(tire_pressure_range[0], tire_pressure_range[1]) + random.gauss(0, sensor_noise) 
                        for _ in range(3)
                        ]
                },
                "transmission_temp": [
                    random.uniform(transmission_temp_range[0], transmission_temp_range[1]) + random.gauss(0, sensor_noise) 
                    for _ in range(3)
                ],
                "suspension_travel": {
                    "FR":[
                        random.uniform(suspension_travel_range[0], suspension_travel_range[1]) + random.gauss(0, sensor_noise) 
                        for _ in range(3)
                    ],
                    "FL": [
                        random.uniform(suspension_travel_range[0], suspension_travel_range[1]) + random.gauss(0, sensor_noise) 
                        for _ in range(3)
                    ],
                    "RR": [
                        random.uniform(suspension_travel_range[0], suspension_travel_range[1]) + random.gauss(0, sensor_noise) 
                        for _ in range(3)
                    ],
                    "RL": [
                        random.uniform(suspension_travel_range[0], suspension_travel_range[1]) + random.gauss(0, sensor_noise) 
                        for _ in range(3)
                    ]
                }
            }

        return vehicle_data 

    def simulate_gps_data(self):
        """Simulates GPS data with multiple sensors per category."""

        # Initial GPS coordinates (example - latitude)
        initial_lat = 37.7749 
        # Initial GPS coordinates (example - longitude)
        initial_lng = -122.4194

        # Define ranges for coordinate offsets (adjust based on desired simulation area)
        lat_offset_range = (-0.0001, 0.0001)  # Adjust for smaller offsets
        lng_offset_range = (-0.00015, 0.00015)  # Adjust for smaller offsets

        # Define compass heading range and noise
        compass_heading_range = (0, 360)
        compass_heading_noise = 5  # Degrees

        # Define altitude range and noise
        altitude_range = (0, 5000)  # Meters
        altitude_noise = 10  # Meters

        # Simulate GPS coordinates with slight offsets
        lat = initial_lat + random.uniform(lat_offset_range[0], lat_offset_range[1])
        lng = initial_lng + random.uniform(lng_offset_range[0], lng_offset_range[1])

        # Simulate compass heading with noise
        compass_heading = random.uniform(compass_heading_range[0], compass_heading_range[1]) + random.gauss(0, compass_heading_noise)

        # Simulate altitude with noise
        altitude = random.uniform(altitude_range[0], altitude_range[1]) + random.gauss(0, altitude_noise)

        # Convert decimal coordinates to degrees, minutes, seconds (DMS)
        lat_dms = self._decimal_to_dms(lat)
        lng_dms = self._decimal_to_dms(lng)

        data = {
            "gps_coordinates": [f"{lat_dms}N, {lng_dms}E" for _ in range(3)], 
            "compass_heading": [compass_heading + random.gauss(0, compass_heading_noise) for _ in range(3)],
            "altitude": [altitude + random.gauss(0, altitude_noise) for _ in range(3)],
        }

        return data

    def simulate_environmental_data(self):
        """Simulates external temperature, barometric pressure, and humidity."""

        # Define sensor ranges and noise levels
        temp_range = (-10, 40)  # Degrees Celsius
        pressure_range = (950, 1050)  # Millibars (mb)
        humidity_range = (0, 100)  # Percentage

        sensor_noise = 0.5  # Standard deviation for sensor noise

        data = {
            "external_temp": [
                random.uniform(temp_range[0], temp_range[1]) + random.gauss(0, sensor_noise) for _ in range(3)
            ],
            "barometric_pressure": [
                random.uniform(pressure_range[0], pressure_range[1]) + random.gauss(0, sensor_noise) for _ in range(3)
            ],
            "humidity": [
                random.uniform(humidity_range[0], humidity_range[1]) + random.gauss(0, sensor_noise) for _ in range(3)
            ],
        }

        return data

    def simulate_crew_vitals(self):
        """Simulates driver and co-pilot vital signs data."""

        # Define sensor ranges and noise levels
        heart_rate_range = (60, 180)  # Beats per minute (BPM)
        oxygen_saturation_range = (95, 100)  # Percentage (%)
        systolic_bp_range = (90, 140)  # Systolic blood pressure (mmHg)
        diastolic_bp_range = (60, 90)  # Diastolic blood pressure (mmHg)

        sensor_noise = 1  # Standard deviation for sensor noise

        data = {
                "driver_heart_rate": random.uniform(heart_rate_range[0], heart_rate_range[1]) + random.gauss(0, sensor_noise),
                "driver_oxygen_saturation": random.uniform(oxygen_saturation_range[0], oxygen_saturation_range[1]) + random.gauss(0, sensor_noise),
                "driver_systolic_bp": random.uniform(systolic_bp_range[0], systolic_bp_range[1]) + random.gauss(0, sensor_noise),
                "driver_diastolic_bp": random.uniform(diastolic_bp_range[0], diastolic_bp_range[1]) + random.gauss(0, sensor_noise),
                "copilot_heart_rate": random.uniform(heart_rate_range[0], heart_rate_range[1]) + random.gauss(0, sensor_noise),
                "copilot_oxygen_saturation": random.uniform(oxygen_saturation_range[0], oxygen_saturation_range[1]) + random.gauss(0, sensor_noise),
                "copilot_systolic_bp": random.uniform(systolic_bp_range[0], systolic_bp_range[1]) + random.gauss(0, sensor_noise),
                "copilot_diastolic_bp": random.uniform(diastolic_bp_range[0], diastolic_bp_range[1]) + random.gauss(0, sensor_noise),
            }

        return data

# This is to be removed when creating the data stream...
if __name__ == '__main__':
    try:
        while True:
            vehicle = DakarRallySimulator()
            data = {'vehicle performance': vehicle.simulate_vehicle_performance(),
                    'location':vehicle.simulate_gps_data(),
                    'weather': vehicle.simulate_environmental_data(),
                    'crew health': vehicle.simulate_crew_vitals()}
            print(data)
            time.sleep(0.5)
    except KeyboardInterrupt:
            print('Terminated by keyboard interruption...')