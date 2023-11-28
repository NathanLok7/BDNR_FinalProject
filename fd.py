#!/usr/bin/env python

"""
Generador de datos para proyecto de Bases de Datos No Relacionales
ITESO 
"""

import argparse
import csv
from random import choice, randint, randrange

airports = ["PDX", "GDL", "SJC", "LAX", "JFK"]
reasons = ["Vacation", "Business", "Moving"]
transports = ["Car rental", "Public Transportation", "Pickup services", "Own car"]

# Line 22
uid_customer = ["PTY"]
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
reasons_custom = ["Business", "Travel", "Work"]
destinations = ["New York", "CDMX", "Madrid"]
is_connection = [0, 1]
takes_flight = [0, 1]
where_to = ["NYC", "CDMX", "MAD"]

dest_airports = ["LA", "BRC", "GDL"]

def generate_dataset(output_file, rows):
    with open(output_file, "w", newline='', encoding='utf-8') as fd:
        fieldnames = ["from", "to", "month", "age", "reason", "transportation", "connection", "dest", "takes_flights", "where_to"]
        fp_dict = csv.DictWriter(fd, fieldnames=fieldnames)
        fp_dict.writeheader()
        for i in range(rows):
            from_airport = choice(airports)
            to_airport = choice(dest_airports)
            reason = choice(reasons_custom)
            month = choice(months)
            connection = choice(is_connection)
            dest = choice(destinations)
            takes_flights = choice(takes_flight)
            transportations = choice(transports)

            line = {
                "from": from_airport,
                "to": to_airport,
                "month": month,
                "age": randint(1, 125),
                "reason": reason,
                "transportation": transportations,
                "connection": connection,
                "dest": dest,
                "takes_flights": takes_flights,
                "where_to": choice(where_to)
            }
            fp_dict.writerow(line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-o", "--output",
                        help="Specify the output filename of your csv, defaults to: flight_passengers.csv",
                        default="flight_passengers.csv")
    parser.add_argument("-r", "--rows",
                        help="Amount of random generated entries for the dataset, defaults to: 100", type=int,
                        default=1000)

    args = parser.parse_args()

    print(f"Generating {args.rows} for flight passenger dataset")
    generate_dataset(args.output, args.rows)
    print(f"Completed generating dataset in {args.output}")
