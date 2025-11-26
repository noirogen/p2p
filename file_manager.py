import os
import math
from bitfield import Bitfield
import threading


class FileManager:
    def __init__(self, my_peer_info, common_config):
        self.peer_id = my_peer_info.peer_id
        self.file_name = common_config["FileName"]
        self.file_size = int(common_config["FileSize"])
        self.piece_size = int(common_config["PieceSize"])
        self.num_pieces = math.ceil(self.file_size / self.piece_size)

        self.peer_dir = f"peer_{self.peer_id}"
        self.file_path = os.path.join(self.peer_dir, self.file_name)
        os.makedirs(self.peer_dir, exist_ok=True)

        self.bitfield = Bitfield(self.num_pieces)
        self.num_pieces_have = 0
        self.file_lock = threading.Lock()

        if my_peer_info.has_file:
            print(f"[{self.peer_id}] Peer starts with the file.")
            self.bitfield.set_all()
            self.num_pieces_have = self.num_pieces
            if not os.path.exists(self.file_path):
                print(
                    f"[{self.peer_id}] WARNING: Seed file not found. Creating empty file."
                )
                with open(self.file_path, "wb") as f:
                    f.seek(self.file_size - 1)
                    f.write(b"\0")
        else:
            print(f"[{self.peer_id}] Peer starts with no pieces.")
            with open(self.file_path, "wb") as f:
                f.seek(self.file_size - 1)
                f.write(b"\0")

        print(f"[{self.peer_id}] File Manager initialized.")
        print(f"[{self.peer_id}] My Bitfield: {self.bitfield}")

    def check_interest(self, their_bitfield):
        return self.bitfield.has_interesting_pieces(their_bitfield)

    def write_piece(self, piece_index, data):
        offset = piece_index * self.piece_size

        with self.file_lock:
            # we had a bug check BEFORE writing or incrementing!
            if self.bitfield.has_piece(piece_index):
                # We already have this piece so we ignore.
                return False

            try:
                with open(self.file_path, "r+b") as f:
                    f.seek(offset)
                    f.write(data)

                # update state only if we didt have it
                self.bitfield.set_piece(piece_index)
                self.num_pieces_have += 1
                return True
            except IOError as e:
                print(f"[{self.peer_id}] ERROR writing piece {piece_index}: {e}")
                return False

    def read_piece(self, piece_index):
        offset = piece_index * self.piece_size
        size = self.piece_size
        if piece_index == self.num_pieces - 1:
            size = self.file_size - offset

        with self.file_lock:
            try:
                with open(self.file_path, "rb") as f:
                    f.seek(offset)
                    data = f.read(size)
                return data
            except IOError as e:
                print(f"[{self.peer_id}] ERROR reading piece {piece_index}: {e}")
                return None

    def is_complete(self):
        return self.num_pieces_have == self.num_pieces