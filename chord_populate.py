"""
@author Edwin Kaburu
@date 11/19/2022
@see "Seattle University, CPSC 5520, Fall 2022" - Distributed Systems
@file chord_populate.py
Lab4 Chord
"""

import csv
import hashlib
import sys
import pickle
import hashlib
from socket import *


def node_get_response_sync(socket_t, set_timer=False):
    try:
        server_host_response = b''
        counter = 0
        while True:
            if set_timer:
                # Set a 5 seconds timeout
                socket_t.settimeout(1)
            # Save to response message to 4096 bytes buffer
            counter += 4096
            try:
                pkt_data = socket_t.recv(4096)
            except TimeoutError:
                break
            else:
                if pkt_data == b'':
                    break
                else:
                    server_host_response += pkt_data

    except Exception as error:
        return None
    else:
        print("Reached")
        # No Errors Raised
        if server_host_response == b'':
            return None
        # return deserialized response
        return pickle.loads(server_host_response)


def node_send_network_message(message, port_num, set_timer=False):
    # Act as a Client, asking An Existing Node To Find Successor
    client_socket = socket(AF_INET, SOCK_STREAM)

    host_response = None
    try:
        # Connect To Existing-Node-Host
        client_socket.connect(('localhost', port_num))
    except Exception as error_msg:
        # Print Exception message
        print(error_msg)
    else:
        # Send Node's Identifier To Existing-Node-Host
        client_socket.send(pickle.dumps(message))

    # Close Client Connection Socket
    client_socket.close()
    # Return Result
    return host_response


class ChordPopulate:
    existing_port = -1
    file_name = ''
    distributed_hash_table = dict()

    def __init__(self, ex_port, fl_name):
        self.existing_port = ex_port
        self.file_name = "{fl_name}"

        if self.existing_port > 0 and self.file_name != '':
            # read csv file data
            self.read_csv_file(fl_name)
            # Distribute DataSet To Nodes
            network_msg = ('Populate', (self.distributed_hash_table, -1))
            node_send_network_message(network_msg, self.existing_port)
        else:
            print("Need Valid Port Number and Valid File Path")

    def read_csv_file(self, file_local):
        with open(file_local, newline='') as csv_file:
            reader_stream = csv.reader(csv_file)
            for row in reader_stream:
                # Treat the value of the firstcolumn (playerid) concatenated with the fourth column (year)
                # as the key and use SHA-1 to hash it
                # Other columns for the row can be put together in a dictionary
                if reader_stream.line_num > 1:
                    self.add_row_to_nfl_dht(row)
                # Print Data Rows
                print(', '.join(row))

    def add_row_to_nfl_dht(self, cr_row: []):
        # Key -> (PlayerID + Year), Value -> (Other Columns in rows)
        row_value = []
        row_key = ""

        # Save The Player Stats
        player_team_stats = []
        # Formulate Key entries to be hashed
        for i in range(0, len(cr_row)):
            element = cr_row[i]
            if i == 0 or i == 3:
                # Entries in Column 0 and 3 make up key
                row_key += element
            else:
                # Save The Rest Entries as Regular Data
                player_team_stats.append(element)

        # Hash Key using SHA-1, then add to distributed hash table
        row_key = hashlib.sha1(row_key.encode())
        # Check if dictionary contains key already
        if row_key.hexdigest() in self.distributed_hash_table:
            # Linear Probing.
            # Get Existing Value from Key
            row_value = self.distributed_hash_table.get(row_key.hexdigest())
            # Append this player's stats to exist value
            row_value.append(player_team_stats)
        else:
            # Append Player's Team Stats To Row Value
            row_value.append(player_team_stats)

        # Save To Hash Table
        self.distributed_hash_table.update({row_key.hexdigest(): row_value})


if __name__ == '__main__':
    print("README:\n\tFile Path should be raw string for instance : "
          "C:\\Users\\EdwinK\\PycharmProjects\\DS\\Career_Stats_Passing.csv")
    # Existing Port Number
    if len(sys.argv) != 3:
        print("Usage: python chord_populate.py EXISTINGPORT FILEPATH")
        exit(1)

    existing_port = int(sys.argv[1])
    file_path = sys.argv[2]

    # Populate Chord Nodes
    population = ChordPopulate(existing_port, file_path)
