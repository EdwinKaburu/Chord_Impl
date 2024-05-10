"""
@author Edwin Kaburu
@date 11/19/2022
@see "Seattle University, CPSC 5520, Fall 2022" - Distributed Systems
@file chord_node.py
Lab4 Chord
"""

import sys
import pickle
import hashlib
from socket import *

SHA1_M_BIT_LENGTH = 160


def get_decimal_form(identifier):
    # Convert Hex-Identifier To Digit
    return int(identifier, 16)


def is_unique(identifier_1, identifier_2):
    if get_decimal_form(identifier_1) == get_decimal_form(identifier_2):
        # Not Unique
        return False
    # Unique
    return True


def get_immediate_indexes(identifier, i_i, summation=True):
    # Convert Hex-Identifier to Digit
    deci_num = get_decimal_form(identifier)
    # Add By Default, To Get Next Successor Halfway
    operation_results = (deci_num + pow(2, (i_i - 1))) % pow(2, SHA1_M_BIT_LENGTH)
    if not summation:
        # Subtract, To Get Predecessor
        operation_results = (deci_num - pow(2, (i_i - 1))) % pow(2, SHA1_M_BIT_LENGTH)
    # Return Result in Hex-decimal form
    return hex(operation_results)


def interval_condition(l_id, x_id, r_id, notation_str):
    # Parameters Are In Hex-Decimal, To Get Insane Large Integer
    l_digit = get_decimal_form(l_id)
    x_digit = get_decimal_form(x_id)
    r_digit = get_decimal_form(r_id)
    # Return Boolean Condition
    if notation_str == '()':
        return l_digit < x_digit < r_digit
    if notation_str == '[)':
        return l_digit <= x_digit < r_digit
    if notation_str == '(]':
        return l_digit < x_digit <= r_digit
    if notation_str == '[]':
        return l_digit <= x_digit <= r_digit


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


class ChordNode:
    # Unique Identifier in Hex-Decimal
    identifier = None
    # Immediate Successor Node
    successor = None
    # Immediate Predecessor Node
    predecessor = None
    # Node Address (name, port)
    listen_address = ()


class ChordProtocol:
    # Single Node Instance Running
    SingleNode = ChordNode()

    # Node's finger table
    finger_table = dict()
    # Node's Listening Socket
    listen_socket = socket()
    # Known Existing Port Value
    existing_port = 0

    nfl_dictionary_table = dict()

    EXCLUSIVE_PORT = -1

    def __init__(self, known_port):
        # Save Existing Port To Join Network
        self.existing_port = known_port
        # Set Up Listening Server
        self.protocol_init_listen_socket()

        # Print Status
        print(f'{self.SingleNode.listen_address}: Joining Network With\n\tDeci-ID-Form: '
              f'{get_decimal_form(self.SingleNode.identifier)}\n\tHex-ID-Form: {self.SingleNode.identifier}')

        self.protocol_join()
        # Print Status
        print(f'{self.SingleNode.listen_address}: ---------- Joined Network ---------')
        # Listen For Incoming Events
        self.protocol_main()

    def protocol_init_listen_socket(self):
        # Create TCP/IP Listening Socket
        self.listen_socket = socket(AF_INET, SOCK_STREAM)
        # Bind the socket to Port and Name
        self.listen_socket.bind(('localhost', 0))
        # Listen To Connections
        self.listen_socket.listen(5)
        # Hash The Node's socket endpoints using SHA-1 function
        self.protocol_hash_endpoints()
        # Debugging Print
        print(f"Node's Listening Server Set Up Complete")

    def protocol_hash_endpoints(self):
        # Get Address String Representation
        end_point = "" + 'localhost' + str(self.listen_socket.getsockname()[1])
        # Set the Listening Address for Node
        self.SingleNode.listen_address = self.listen_socket.getsockname()
        # Store Hex-Decimal Format
        # self.SingleNode.identifier = hashlib.sha1(end_point.encode()).hexdigest()

        simple_digit = get_decimal_form(hashlib.sha1(end_point.encode()).hexdigest())
        simple_digit = simple_digit % pow(2, SHA1_M_BIT_LENGTH)
        # Simple
        self.SingleNode.identifier = hex(simple_digit)

    def protocol_find_successor(self, identifier):
        # Call Predecessor
        node_prime = self.protocol_find_predecessor(identifier)
        if node_prime is None:
            return self.protocol_find_lowest_node()
        # Return Successor
        return node_prime.successor

    def protocol_find_lowest_node(self):
        unique_predecessor = is_unique(self.SingleNode.predecessor.identifier, self.SingleNode.identifier)

        if unique_predecessor and self.EXCLUSIVE_PORT != self.SingleNode.predecessor.listen_address[1]:
            print('Finding Lowest Node')
            lowest_message = ('FindLowestNode', (self.SingleNode.identifier, self.EXCLUSIVE_PORT))
            # Find Lowest Node
            node_prime = node_send_network_message(lowest_message, self.SingleNode.predecessor.listen_address[1])
            if node_prime is not None:
                print("None Found")
                return node_prime
        # We are The Lowest Node
        return self.SingleNode

    def protocol_find_predecessor(self, identifier):

        unique_predecessor = is_unique(self.SingleNode.predecessor.identifier, self.SingleNode.identifier)
        unique_successor = is_unique(self.SingleNode.successor.identifier, self.SingleNode.identifier)

        # Check Interval Condition of Identifier
        # x < identifier <= z . O(1) Complex, in all cases
        if interval_condition(self.SingleNode.identifier, identifier, self.SingleNode.successor.identifier, '[]'):
            # Return Single Node Instance
            return self.SingleNode

        # identifier < x <= z . O(n) Complex, in worst case
        if interval_condition(identifier, self.SingleNode.identifier, self.SingleNode.successor.identifier, '(]'):
            # Get Predecessor Node
            node_prime = self.SingleNode.predecessor
            if unique_predecessor and self.EXCLUSIVE_PORT != node_prime.listen_address[1]:
                print(f'Asking Predecessor: {node_prime.identifier}')
                predecessor_message = ('FindPredecessor', (identifier, self.EXCLUSIVE_PORT))
                # Ask Predecessor Pointer (for Predecessor To Identifier)
                node_prime = node_send_network_message(predecessor_message, node_prime.listen_address[1])

            if node_prime is not None and get_decimal_form(identifier) >= get_decimal_form(node_prime.identifier):
                # Return Result
                return node_prime

            if get_decimal_form(identifier) >= get_decimal_form(self.SingleNode.predecessor.identifier):
                return self.SingleNode.predecessor

        # x <= z <= identifier. O(n) Complex, in worst case
        if interval_condition(self.SingleNode.identifier, self.SingleNode.successor.identifier, identifier, '[]'):
            # Get Successor Node
            node_prime = self.SingleNode.successor

            if unique_successor and self.EXCLUSIVE_PORT != node_prime.listen_address[1]:
                print(f'Asking Successor: {node_prime.identifier}')
                # Formulate Message
                predecessor_message = ('FindPredecessor', (identifier, self.EXCLUSIVE_PORT))
                # Contact And Send Message To Successor Pointer
                node_prime = node_send_network_message(predecessor_message, node_prime.listen_address[1])

            if node_prime is not None and get_decimal_form(identifier) >= get_decimal_form(node_prime.identifier):
                # Return Result
                return node_prime

            if get_decimal_form(identifier) >= get_decimal_form(self.SingleNode.successor.identifier):
                return self.SingleNode.successor

        # Get The Closest Proceeding Node. O(log-n) Complex, in worst case
        closest_proceed_node = self.protocol_closest_proceeding_finger(identifier)
        if closest_proceed_node is not None and is_unique(closest_proceed_node.identifier, self.SingleNode.identifier) \
                and self.EXCLUSIVE_PORT != closest_proceed_node.listen_address[1]:

            print(f'Asking Closest Node: {closest_proceed_node.identifier}')
            # Formulate Message
            predecessor_message = ('FindPredecessor', (identifier, self.EXCLUSIVE_PORT))
            # Contact And Send Message To The Closest Proceeding Node
            node_prime = node_send_network_message(predecessor_message, closest_proceed_node.listen_address[1])

            if node_prime is not None and get_decimal_form(identifier) >= get_decimal_form(node_prime.identifier):
                # Return Result
                return node_prime

    def protocol_closest_proceeding_finger(self, identifier):
        # closest proceeding node
        closest_proceed_node = self.SingleNode
        # Iterate Through Dictionary
        for key, value in self.finger_table.items():
            # Validate Interval Notation
            condition_check = interval_condition(self.SingleNode.identifier, key, identifier, '()')
            if condition_check:
                # Update Closest Proceeding Node
                closest_proceed_node = value
        # Return Closest-Proceeding Node
        return closest_proceed_node

    def protocol_join(self):
        # Reset Exclusive Port
        self.EXCLUSIVE_PORT = self.SingleNode.listen_address[1]
        # Validate Port Number
        if self.existing_port == 0:
            # Print Status
            print(f'{self.SingleNode.listen_address}: [Only] Node In Network')
            # Only Node in Network
            i = 1
            while i <= SHA1_M_BIT_LENGTH:
                # Calculate Half-Way Potential Identifier
                halfway_identifier = get_immediate_indexes(self.SingleNode.identifier, i)
                # Set Key-Value To SingleNode Instance
                self.finger_table.update({halfway_identifier: self.SingleNode})
                i += 1
            # Set Predecessor To Ourselves
            self.SingleNode.predecessor = self.SingleNode
            # Set Successor To Ourselves
            self.SingleNode.successor = self.SingleNode
        else:
            # Print Status
            print(f'{self.SingleNode.listen_address}: [Not] Only Node In Network')
            # Init Finger Table
            self.protocol_init_finger_table()

            if get_decimal_form(self.SingleNode.identifier) != get_decimal_form(self.SingleNode.predecessor.identifier):
                # Update Others
                self.protocol_update_others()
        self.EXCLUSIVE_PORT = -1

    def protocol_init_finger_table(self):
        i = 1
        # Immediate Halfway Identifier Successor
        halfway_identifier = get_immediate_indexes(self.SingleNode.identifier, i)
        # Formulate Message
        successor_msg = ("FindSuccessor", (self.SingleNode.identifier, self.EXCLUSIVE_PORT))
        # Get Successor Node
        successor_node = node_send_network_message(successor_msg, self.existing_port)

        # Validate Successor Node is Greater
        if get_decimal_form(successor_node.identifier) > get_decimal_form(self.SingleNode.identifier):
            # Print Status
            print(f'{self.SingleNode.listen_address}:'
                  f' Init-Finger-Table, Successor Node is {successor_node.listen_address}')

            # Update Predecessor Pointer
            self.SingleNode.predecessor = successor_node.predecessor
            # Update Successor Pointer
            self.SingleNode.successor = successor_node

            unique_cond = is_unique(successor_node.predecessor.identifier, successor_node.identifier)

            # Validate The Successor Node and Its Predecessor Pointer Have Different Identifiers
            if unique_cond is False:
                # Modify Predecessor Pointer To Ourselves
                self.SingleNode.predecessor = self.SingleNode

            # Formulate Message
            update_predecessor_msg = ("UpdatePredecessor", (self.SingleNode, self.EXCLUSIVE_PORT))
            # Inform Successor of Predecessor Change
            self.SingleNode.successor = node_send_network_message(update_predecessor_msg,
                                                                  self.SingleNode.successor.listen_address[1])

            if unique_cond:
                # Formulate Message
                update_successor_msg = ("UpdateSuccessor", (self.SingleNode, self.EXCLUSIVE_PORT))
                # Inform Predecessor of Immediate Change
                self.SingleNode.predecessor = node_send_network_message(update_successor_msg,
                                                                        self.SingleNode.predecessor.listen_address[1])

            # Update Finger-Table with Immediate Successor Node
            self.finger_table.update({halfway_identifier: self.SingleNode.successor})

            print("Setting Up Finger Table")
            while i < SHA1_M_BIT_LENGTH:
                # current i node
                current_i_finger_node = self.finger_table.get(get_immediate_indexes(self.SingleNode.identifier, i))
                # Next Identifier Key
                next_i_finger_index = get_immediate_indexes(self.SingleNode.identifier, (i + 1))

                if current_i_finger_node is not None and interval_condition(self.SingleNode.identifier,
                                                                            next_i_finger_index,
                                                                            current_i_finger_node.identifier, '[)'):
                    # Update Finger Table at Next Identifier
                    self.finger_table.update({next_i_finger_index: current_i_finger_node})

                elif current_i_finger_node is not None and interval_condition(self.SingleNode.identifier,
                                                                              current_i_finger_node.identifier,
                                                                              next_i_finger_index, '(]'):
                    # Ask Successor
                    if is_unique(self.SingleNode.identifier, next_i_finger_index):
                        # Find next-halfway-identifier successor
                        successor_msg = ("FindSuccessor", (next_i_finger_index, self.EXCLUSIVE_PORT))
                        # Get Successor Node
                        successor_node = node_send_network_message(successor_msg, self.existing_port)
                        # Update Finger Table To New Successor
                        self.finger_table.update({next_i_finger_index: successor_node})
                else:

                    # Update Finger Table To New Successor
                    self.finger_table.update({next_i_finger_index: self.SingleNode})

                # Increment Index
                i += 1
        else:
            # Print Status
            print(f'{self.SingleNode.listen_address}: Init-Finger-Table,'
                  f' Predecessor Node is {successor_node.listen_address}')

            # No Successors Only Predecessors In Network
            i = 1
            while i <= SHA1_M_BIT_LENGTH:
                # Calculate Half-Way Potential Identifier
                halfway_identifier = get_immediate_indexes(self.SingleNode.identifier, i)
                # Set Key-Value To SingleNode Instance
                self.finger_table.update({halfway_identifier: self.SingleNode})
                # Increment
                i += 1

            # Update Predecessor Pointer
            self.SingleNode.predecessor = successor_node

            # Inform Predecessor Node, To UpdateSuccessor
            upt_successor_msg = ("UpdateSuccessor", (self.SingleNode, self.EXCLUSIVE_PORT))
            # Get Successor Node
            self.SingleNode.predecessor = node_send_network_message(upt_successor_msg, successor_node.listen_address[1])

            # Update Successor Pointer
            self.SingleNode.successor = self.SingleNode

    def protocol_update_others(self):
        # Update All Nodes Whose Finger Table Should refer To current Node
        # Print Status
        print(f'{self.SingleNode.listen_address}: Update-Others, Whose Finger Table Should Refer To N')
        i = 1
        while i < SHA1_M_BIT_LENGTH:
            # Previous Halfway Identifier
            pr_half_way_identifier = get_immediate_indexes(self.SingleNode.identifier, i, False)

            if pr_half_way_identifier in self.finger_table:
                print("Skip Tim")

            # Formulate Message And Get Predecessor Over Network
            predecessor_message = ('FindPredecessor', (pr_half_way_identifier, self.EXCLUSIVE_PORT))

            # Contact And Send Message To The Closest Proceeding Node
            predecessor_node = node_send_network_message(predecessor_message, self.existing_port)

            # Do Not Send Message To Ourselves
            if predecessor_node is not None and is_unique(predecessor_node.identifier, self.SingleNode.identifier):
                # Print Status
                print(f'{self.SingleNode.listen_address}: Informing {predecessor_node.listen_address} '
                      f'To Update Finger Table')
                # Formulate Message
                update_finger_msg = ("UpdateFinger", (self.SingleNode, i, self.EXCLUSIVE_PORT))
                # Inform Predecessor To Update Finger Table
                node_send_network_message(update_finger_msg, predecessor_node.listen_address[1])

            # Increment I-Index
            i += 1

    def protocol_update_finger_table(self, s, i):
        # Half Way Identifier
        half_way_identifier = get_immediate_indexes(self.SingleNode.identifier, i)
        # Get Finger Node at Identifier
        # i_finger_node = self.finger_table.get(half_way_identifier)

        # Validate Node-S
        if get_decimal_form(s.identifier) == get_decimal_form(half_way_identifier):
            # Print Status
            print(f'{self.SingleNode.listen_address}: Updating Finger Table at {i} th')
            # Great, Node-S, is in-between the conditions: Modify finger Node
            self.finger_table.update({half_way_identifier: s})

        # Get Immediate Predecessor : First Node Preceding N
        predecessor_node = self.SingleNode.predecessor
        # Do Not Send Message To Ourselves or Successor

        if is_unique(predecessor_node.identifier, self.SingleNode.predecessor.identifier) \
                and is_unique(predecessor_node.identifier, s.identifier):
            print(f"Sending UpdateFinger to {predecessor_node.identifier}")
            # Formulate Message
            update_finger_msg = ("UpdateFinger", (s, i, self.EXCLUSIVE_PORT))
            # Inform Predecessor To Update Finger Table
            node_send_network_message(update_finger_msg, self.SingleNode.predecessor.listen_address[1])

    def protocol_main(self):
        # Print Status
        print(f'{self.SingleNode.listen_address}: Ready And Listening For Events')
        while True:
            self.EXCLUSIVE_PORT = -1
            # Accept Connection
            client_con_socket, client_con_address = self.listen_socket.accept()
            # Decode the Message from Client Connection Socket
            client_con_data = node_get_response_sync(client_con_socket, True)

            if client_con_data is not None:
                # Event Handler Callback
                response_message = self.protocol_event_handler(client_con_data)
                # Send Response Back To Client
                client_con_socket.send(pickle.dumps(response_message))

            # close connection
            client_con_socket.close()

    def protocol_new_successor(self, possible_successor):
        # Check if the current node and or successor are identical
        unique_cond = is_unique(self.SingleNode.identifier, self.SingleNode.successor.identifier)

        if unique_cond is False or interval_condition(self.SingleNode.identifier, possible_successor.identifier,
                                                      self.SingleNode.successor.identifier, '[]'):
            # Print Status
            print(f'{self.SingleNode.listen_address}:'
                  f' Updating Immediate Successor Pointer, To {possible_successor.listen_address}')
            # Update Immediate Successor Pointer
            self.SingleNode.successor = possible_successor
            # Update Finger Table
            self.finger_table.update({get_immediate_indexes(self.SingleNode.identifier, 1): self.SingleNode.successor})
        # Return Node
        return self.SingleNode

    def protocol_new_predecessor(self, possible_predecessor):
        # Print Status
        print(f'{self.SingleNode.listen_address}: Updating Immediate Predecessor Pointer,'
              f'To {possible_predecessor.listen_address}')
        # Update Predecessor Pointer
        self.SingleNode.predecessor = possible_predecessor
        # Return Node
        return self.SingleNode

    def protocol_populate_nfl(self, dht_data_set: dict):
        lower_point = self.SingleNode.identifier
        highest_point = self.SingleNode.successor.identifier
        for key, value in dht_data_set.items():
            if interval_condition(lower_point, key, highest_point, '[)') and \
                    is_unique(self.SingleNode.identifier, self.SingleNode.successor.identifier):
                # In Between x <= key < y
                self.nfl_dictionary_table.update({key: value})

            if interval_condition(lower_point, highest_point, key, '[]') and \
                    is_unique(self.SingleNode.identifier, self.SingleNode.successor.identifier) is False:
                # In Between x <= key <= y
                self.nfl_dictionary_table.update({key: value})

            if interval_condition(key, lower_point, highest_point, '[]') and \
                    is_unique(self.SingleNode.identifier, self.SingleNode.predecessor.identifier) is False:
                # In Between  key <= x <= y
                self.nfl_dictionary_table.update({key: value})

        # Need To Send Data To Predecessor and Successor
        if is_unique(self.SingleNode.identifier, self.SingleNode.predecessor.identifier) and \
                self.EXCLUSIVE_PORT != self.SingleNode.predecessor.listen_address[1]:
            # Don't send message back to ourselves.
            network_msg = ('Populate', (dht_data_set, self.SingleNode.listen_address[1]))
            # Send Message To Predecessor
            node_send_network_message(network_msg, self.SingleNode.predecessor.listen_address[1])

        if is_unique(self.SingleNode.identifier, self.SingleNode.successor.identifier) and \
                self.EXCLUSIVE_PORT != self.SingleNode.successor.listen_address[1]:
            # Don't send message back to ourselves.
            network_msg = ('Populate', (dht_data_set, self.SingleNode.listen_address[1]))
            # Send Message To Predecessor
            node_send_network_message(network_msg, self.SingleNode.successor.listen_address[1])

        print(f'{self.SingleNode.listen_address}: Dataset Entries Populated')

    def protocol_find_record(self, identifier):
        # Check if we have the record
        if identifier in self.nfl_dictionary_table:
            # Return the Result
            return self.nfl_dictionary_table.get(identifier)

        data_record = "Not Here"
        # Need To Send Data To Predecessor and Successor
        if is_unique(self.SingleNode.identifier, self.SingleNode.predecessor.identifier) and \
                self.EXCLUSIVE_PORT != self.SingleNode.predecessor.listen_address[1]:
            # Don't send message back to ourselves.
            network_msg = ('FindKey', (identifier, self.SingleNode.listen_address[1]))
            # Send Message To Predecessor
            data_record = node_send_network_message(network_msg, self.SingleNode.predecessor.listen_address[1])

        if data_record == "Not Here" and is_unique(self.SingleNode.identifier, self.SingleNode.successor.identifier) \
                and self.EXCLUSIVE_PORT != self.SingleNode.successor.listen_address[1]:
            # Don't send message back to ourselves.
            network_msg = ('FindKey', (identifier, self.SingleNode.listen_address[1]))
            # Send Message To Predecessor
            node_send_network_message(network_msg, self.SingleNode.successor.listen_address[1])

        return data_record

    def protocol_event_handler(self, client_data):
        # Get Method
        rpc_method = client_data[0]
        # Get Rpc Data
        rpc_data = client_data[1]
        # Extra Information
        rpc_params = None

        if len(rpc_data) == 2:
            # Get Exclusive Port
            self.EXCLUSIVE_PORT = rpc_data[1]
            # Get Data
            rpc_params = rpc_data[0]
        if len(rpc_data) == 3:
            # Get Exclusive Port
            self.EXCLUSIVE_PORT = rpc_data[2]
            # Get Data
            rpc_params = (rpc_data[0], rpc_data[1])

        if rpc_params != None:
            if rpc_method == 'UpdateFinger':
                # Protocol-Update-Finger-Table RPC
                self.protocol_update_finger_table(rpc_params[0], rpc_params[1])
            if rpc_method == 'UpdateSuccessor':
                # New Update Successor
                return self.protocol_new_successor(rpc_params)
            if rpc_method == 'FindSuccessor':
                # Protocol-Find-Successor RPC
                return self.protocol_find_successor(rpc_params)
            if rpc_method == 'FindPredecessor':
                # Protocol-Find-Predecessor RPC
                return self.protocol_find_predecessor(rpc_params)
            if rpc_method == 'UpdatePredecessor':
                # New Update Predecessor
                return self.protocol_new_predecessor(rpc_params)
            if rpc_method == 'FindLowestNode':
                # Return Node with the lowest Identifier in Network
                return self.protocol_find_lowest_node()
            if rpc_method == 'Populate':
                self.protocol_populate_nfl(rpc_params)
            if rpc_method == 'FindKey':
                return self.protocol_find_record(rpc_params)
        else:
            print("Something Happened")

        # Return Node For Any Events
        return self.SingleNode


if __name__ == '__main__':
    print("README:\n\tProvide Port Number: Port Number Zero[0] reserve for starting network")
    # Existing Port Number
    if len(sys.argv) != 2:
        print("Usage: python chord_node.py EXISTINGPORT")
        exit(1)

    # Get Port Number from Entry
    port_number = int(sys.argv[1])
    # Call Method
    chord_protocol = ChordProtocol(port_number)

    print('End of Program')
