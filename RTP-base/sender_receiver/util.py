import zlib

def compute_checksum(packet):
    return zlib.crc32(packet) & 0xffffffff
