"""
@author Edwin Kaburu
@date 11/19/2022
@see "Seattle University, CPSC 5520, Fall 2022" - Distributed Systems
@file chord_query.py
Lab4 Chord
"""

import sys
import select
import pickle
import hashlib
import threading
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
        # No Errors Raised
        if server_host_response == b'':
            return None
        # return deserialized response
        return pickle.loads(server_host_response)


def node_send_network_message(message, port_num, set_timer=False):
    # Act as a Client, asking An Existing Node To Find Successor
    client_socket = socket(AF_INET, SOCK_STREAM)
    # set socket timeout to 1.5 seconds
    # client_socket.settimeout(2)
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
        # Get Response from Existing-Node-Host
        host_response = node_get_response_sync(client_socket, set_timer)

    # Close Client Connection Socket
    client_socket.close()
    # Return Result
    return host_response


if __name__ == '__main__':
    print("README:\n\tProvide Port Number, Along with Column1 and Column4 values from csv file.\n\t"
          "Column1 and Column4 will be used to hashed to identifier."
          "\nExample\n\tPort: 51544\tColumn1:billdemory/2512778\tColumn2:1974")

    # Existing Port Number
    if len(sys.argv) != 4:
        print("Usage: python chord_populate.py EXISTINGPORT COLUMN1 COLUMN2")
        exit(1)

    existing_port = int(sys.argv[1])
    column1 = sys.argv[2]
    column2 = int(sys.argv[3])

    row_key = column1 + str(column2)
    # Hash Key using SHA-1, then add to distributed hash table
    row_key = hashlib.sha1(row_key.encode()).hexdigest()

    # Distribute DataSet To Nodes
    network_msg = ('FindKey', (row_key, -1))
    result = node_send_network_message(network_msg, existing_port)

    print(result)
