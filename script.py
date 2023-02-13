#!/usr/bin/env python3

import sys
import asyncio
import csv
import Levenshtein
from geopy.distance import great_circle
import argparse
from tqdm import tqdm


MAX_DISTANCE = 200
MAX_EDITS = 5
CHUNKS = 100000


async def distance_between_coordinates(coord1, coord2):
    """
    Calculates the distance between two coordinate pairs.

    Parameters:
    coord1 (tuple): The first coordinate pair.
    coord2 (tuple): The second coordinate pair.

    Returns:
    float: The distance between the two coordinates in meters.
    """
    return great_circle(coord1, coord2).meters


async def is_name_similar(name1, name2):
    """
    Determines if two names are similar.

    Parameters:
    name1 (str): The first name.
    name2 (str): The second name.

    Returns:
    bool: True if the names are similar, False otherwise.
    """
    return Levenshtein.distance(name1, name2) < MAX_EDITS


async def process_entries(i, j, entries):
    """
    Processes two entries and marks them as similar if they meet the similarity criteria.

    Parameters:
    i (int): The index of the first entry.
    j (int): The index of the second entry.
    entries (list): The list of entries.
    """
    entry1 = entries[i]
    entry2 = entries[j]

    # Converting the data into desired format
    coord1 = (entry1['latitude'], entry1['longitude'])
    coord2 = (entry2['latitude'], entry2['longitude'])
    distance = await distance_between_coordinates(coord1, coord2)

    # If distance is more then no need for further processing
    if distance > MAX_DISTANCE:
        return

    name_similar = await is_name_similar(entry1['name'], entry2['name'])
    # If name is not similar then no need for further processing
    if not name_similar:
        return

    # Marking both entries as 1
    entries[i]['is_similar'] = 1
    entries[j]['is_similar'] = 1


async def find_similar_entries(entries):
    """
    This function finds all similar entries in a list of entries.
    Processing is done per CHUNKS chunks

    Parameters:
    entries (list): The list of entries.
    """
    # Initialize the variables
    n = len(entries)
    tasks = []
    counter = 0
    # Looping from 1 to n and for per entry we check for all other entries
    # because any entry can satisfy the conditions
    # tqdm for progress bar
    for i in tqdm(range(n)):
        # Check if already marked 1 to reduce un
        if entries[i]['is_similar'] == 1:
            continue
        for j in range(i+1, n):
            # Create task and store it to tasks
            task = asyncio.create_task(process_entries(i, j, entries))
            tasks.append(task)
            counter += 1
            # Process tasks and reset the counter
            if counter == CHUNKS:
                await asyncio.gather(*tasks)
                counter = 0
                tasks = []

    # Check if some tasks are remaining do to exiting the loop and execute
    # then if they are
    if counter != 0:
        await asyncio.gather(*tasks)


async def main(input_file, output_file):
    """
    Main function for processing data.

    Parameters:
    input_file (str): The path to the input file.
    output_file (str): The path to the output file.
    """
    entries = []
    # Input the file and store it in entries array
    # (All the data needs to be stored in entries because of
    # similarity constraint in problem statement)
    try:
        with open(input_file, 'r') as file:
            file_reader = csv.DictReader(file)
            for row in file_reader:
                entries.append({
                    'name': row['name'],
                    'latitude': float(row['latitude']),
                    'longitude': float(row['longitude']),
                    'is_similar': 0
                })
    except Exception as e:
        print("Following column not found in input file", e)
        sys.exit(0)

    # A simple check if want to proceed or not
    print(len(entries), "entries will to be processed")
    proceed = input("Starting processing? [Y/N]: ")
    while proceed.lower() != 'y' or proceed.lower() != 'n':
        if proceed == 'n':
            sys.exit(0)
        elif proceed == 'y':
            break
        proceed = input("Please enter valid input [Y/N]: ")

    # Calling the function
    await find_similar_entries(entries)

    # Writing to file
    try:
        with open(output_file, 'w') as file:
            writer = csv.DictWriter(
                file, fieldnames=["name", "latitude", "longitude", "is_similar"])
            writer.writeheader()
            for entry in entries:
                writer.writerow(entry)
    except Exception as e:
        print(
            f"An error occurred while writing to the file {output_file}: {e}")

    print("Data Written to file: ", output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=" \
    Spatic assignment: \
    The following script takes input a csv inputfile and converts \
    the data according to the problem statement. \
    The script uses asyncio for fast processing of data \
    and divides the tasks into a chunks of CHUNKS (default=100000) \
    ")
    parser.add_argument('input_file', metavar='input_file',
                        type=str, help='Input CSV file name')
    parser.add_argument('output_file', metavar='output_file',
                        type=str, help='Output CSV file name')

    args = parser.parse_args()
    input_file = args.input_file
    output_file = args.output_file
    # Check if file even exists or not
    try:
        open(input_file)
    except FileNotFoundError:
        print(f"File Not Found!")
        sys.exit(0)
    asyncio.run(main(input_file, output_file))
