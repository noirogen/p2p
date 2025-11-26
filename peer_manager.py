import time
import threading
import random
from itertools import groupby  # added for tie breaking
from logger import log_preferred_neighbors, log_optimistic_neighbor


class PeerManager:
    # Added all_peer_info param to track termination state
    def __init__(self, my_peer_id, all_peers_info, file_manager, common_config):
        self.my_peer_id = my_peer_id
        self.file_manager = file_manager

        self.k = int(common_config["NumberOfPreferredNeighbors"])
        self.p_interval = int(common_config["UnchokingInterval"])
        self.m_interval = int(common_config["OptimisticUnchokingInterval"])

        self.connections = {}
        self.preferred_neighbors = set()
        self.optimistic_neighbor = None

        self.all_peers_info = all_peers_info

        # store all bitfields
        self.peer_bitfields = {
            p.peer_id: file_manager.bitfield if p.peer_id == my_peer_id else None
            for p in all_peers_info
        }
        self.shutdown_event = threading.Event()

        self.lock = threading.Lock()

        print(f"[{my_peer_id}] PeerManager initialized.")

    def add_connection(self, peer_id, handler_thread):
        with self.lock:
            self.connections[peer_id] = handler_thread
        print(f"[{self.my_peer_id}] PeerManager registered connection with {peer_id}.")

    def remove_connection(self, peer_id):
        with self.lock:
            if peer_id in self.connections:
                del self.connections[peer_id]
            self.preferred_neighbors.discard(peer_id)
            if self.optimistic_neighbor == peer_id:
                self.optimistic_neighbor = None
        print(f"[{self.my_peer_id}] PeerManager removed connection with {peer_id}.")

    def start_timers(self):
        threading.Thread(target=self._preferred_neighbor_timer, daemon=True).start()
        threading.Thread(target=self._optimistic_neighbor_timer, daemon=True).start()

    def _preferred_neighbor_timer(self):
        while not self.shutdown_event.is_set():
            time.sleep(self.p_interval)

            with self.lock:
                if self.shutdown_event.is_set():
                    break

                # --- 1. HANDLE RANDOM SELECTION IF FILE IS COMPLETE ---
                # If we have the complete file, download rates don't matter we pick neighbors randomly from those who are interested.
                if self.file_manager.is_complete():
                    # print(f"[{self.my_peer_id}] (File complete, selecting neighbors randomly)")
                    interested_ids = [
                        pid
                        for pid, h in self.connections.items()
                        if h.is_interested_in_me
                    ]
                    random.shuffle(interested_ids)
                    new_preferred_set = set(interested_ids[: self.k])

                # --- 2. HANDLE RATE-BASED SELECTION WITH TIE-BREAKING ---
                else:
                    # we get all interested peers with their rates
                    interested_peers = []
                    for peer_id, handler in self.connections.items():
                        if handler.is_interested_in_me:
                            rate = handler.get_download_rate()
                            interested_peers.append((rate, peer_id))

                    # (B) Sort them by download rate (highest first) to prepare for grouping
                    interested_peers.sort(key=lambda x: x[0], reverse=True)

                    # (C) Select top k with random tie-breaking
                    new_preferred_set = set()

                    # Group peers by their download rate
                    for rate, group in groupby(interested_peers, key=lambda x: x[0]):

                        # Get the peer ids from the group
                        peer_ids_in_group = [peer_id for r, peer_id in group]

                        remaining_slots = self.k - len(new_preferred_set)

                        # If the whole group fits we add them all
                        if len(peer_ids_in_group) <= remaining_slots:
                            new_preferred_set.update(peer_ids_in_group)
                        # If the group is too big (which implies a tie), shuffle and pick randomly
                        else:
                            random.shuffle(peer_ids_in_group)
                            new_preferred_set.update(
                                peer_ids_in_group[:remaining_slots]
                            )

                        # Stop once we've filled k slots
                        if len(new_preferred_set) >= self.k:
                            break

                # --- 3. SEND CHOKE/UNCHOKE MESSAGES ---
                peers_to_unchoke = new_preferred_set - self.preferred_neighbors
                peers_to_choke = self.preferred_neighbors - new_preferred_set

                for peer_id in peers_to_unchoke:
                    if (
                        peer_id in self.connections
                        and self.connections[peer_id].am_choking_them
                    ):
                        self.connections[peer_id].send_unchoke()

                for peer_id in peers_to_choke:
                    if (
                        peer_id in self.connections
                        and peer_id != self.optimistic_neighbor
                    ):
                        if not self.connections[peer_id].am_choking_them:
                            self.connections[peer_id].send_choke()

                self.preferred_neighbors = new_preferred_set
                log_preferred_neighbors(self.my_peer_id, list(new_preferred_set))

    def _optimistic_neighbor_timer(self):
        while not self.shutdown_event.is_set():
            time.sleep(self.m_interval)
            with self.lock:
                if self.shutdown_event.is_set():
                    break

                eligible_peers = []
                for peer_id, handler in self.connections.items():
                    # reselects andomly among neighbors that are choked but are interested
                    if (
                        handler.is_interested_in_me
                        and handler.am_choking_them
                        and peer_id not in self.preferred_neighbors
                    ):
                        eligible_peers.append(peer_id)

                if eligible_peers:
                    new_optimistic_neighbor = random.choice(eligible_peers)

                    # Choke the old one if needed
                    if (
                        self.optimistic_neighbor
                        and self.optimistic_neighbor in self.connections
                        and self.optimistic_neighbor not in self.preferred_neighbors
                        and not self.connections[
                            self.optimistic_neighbor
                        ].am_choking_them
                    ):
                        self.connections[self.optimistic_neighbor].send_choke()

                    # Unchoke the new one
                    self.optimistic_neighbor = new_optimistic_neighbor
                    if (
                        self.optimistic_neighbor in self.connections
                        and self.connections[self.optimistic_neighbor].am_choking_them
                    ):
                        self.connections[self.optimistic_neighbor].send_unchoke()

                    log_optimistic_neighbor(self.my_peer_id, self.optimistic_neighbor)

    # Broadcasts to all pieces what current pieces it has
    def broadcast_have(self, piece_index):
        print(f"[{self.my_peer_id}] Broadcasting HAVE {piece_index} to all peers.")
        with self.lock:
            for peer_id, handler in self.connections.items():
                handler.send_have(piece_index)

    # Called by a ConnectionHandler when it receives a
    # BITFIELD or HAVE message.
    def update_peer_bitfield(self, peer_id, bitfield):
        with self.lock:
            self.peer_bitfields[peer_id] = bitfield
            self._check_for_termination()

    # Checks if all peers (from the original PeerInfo.cfg)
    # have the complete file. If so, triggers shutdown.
    def _check_for_termination(self):
        num_pieces = self.file_manager.num_pieces

        for peer_info in self.all_peers_info:
            peer_id = peer_info.peer_id
            bfield = self.peer_bitfields.get(peer_id)

            if bfield is None:  # if we dont have bitfield we aint done
                return

            # double check each piece is present
            for i in range(num_pieces):
                if not bfield.has_piece(i):
                    return  # found a peer that is not done

        # this means it has passed all checks.
        print(f"[{self.my_peer_id}] All peers have completed the download!")
        self.shutdown_event.set()
