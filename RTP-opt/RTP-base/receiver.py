#24020632_ Nguyen Trong Van Son
import socket
import sys
import struct
from sender_receiver.util import compute_checksum

def chay_receiver(cong_ket_noi, kich_thuoc_cua_so):
    socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_udp.bind(('0.0.0.0', cong_ket_noi))

    so_thu_tu_mong_doi = 1
    kho_dem_goi_lech_thu_tu = {}
    dia_chi_sender_hien_tai = None
    du_lieu_nhan_duoc = bytearray()
    danh_sach_payload_theo_seq = {}
    
    da_nhan_goi_end = False
    seq_end = -1

    while True:
        try:
            socket_udp.settimeout(1.0)
            goi_tin, dia_chi_client = socket_udp.recvfrom(2048)
        except socket.timeout:
            if da_nhan_goi_end and len(kho_dem_goi_lech_thu_tu) == 0:
                break
            continue

        if len(goi_tin) < 16:
            continue

        loai_goi, so_thu_tu, do_dai, checksum_nhan = struct.unpack('i i i I', goi_tin[:16])
        payload = goi_tin[16:16+do_dai]

        # Kiểm tra tính toàn vẹn bằng checksum
        goi_tam_tinh = struct.pack('i i i I', loai_goi, so_thu_tu, do_dai, 0) + payload
        if compute_checksum(goi_tam_tinh) != checksum_nhan:
            continue

        # Xử lý gói START (type = 0)
        if loai_goi == 0:
            dia_chi_sender_hien_tai = dia_chi_client
            goi_ack = struct.pack('i i i I', 3, 1, 0, 0)
            goi_ack_checksum = compute_checksum(goi_ack + b"")
            goi_ack = struct.pack('i i i I', 3, 1, 0, goi_ack_checksum)
            socket_udp.sendto(goi_ack, dia_chi_client)
            continue

        if dia_chi_client != dia_chi_sender_hien_tai:
            continue

        # Xử lý gói DATA (type = 2)
        if loai_goi == 2:
            if so_thu_tu >= so_thu_tu_mong_doi + kich_thuoc_cua_so:
                continue

            if so_thu_tu >= so_thu_tu_mong_doi:
                if so_thu_tu not in danh_sach_payload_theo_seq:
                    danh_sach_payload_theo_seq[so_thu_tu] = payload

                while so_thu_tu_mong_doi in danh_sach_payload_theo_seq:
                    du_lieu_nhan_duoc.extend(danh_sach_payload_theo_seq.pop(so_thu_tu_mong_doi))
                    so_thu_tu_mong_doi += 1

            # Gửi Cumulative ACK
            goi_ack = struct.pack('i i i I', 3, so_thu_tu_mong_doi, 0, 0)
            goi_ack_checksum = compute_checksum(goi_ack + b"")
            goi_ack = struct.pack('i i i I', 3, so_thu_tu_mong_doi, 0, goi_ack_checksum)
            socket_udp.sendto(goi_ack, dia_chi_client)

        # Xử lý gói END (type = 1)
        elif loai_goi == 1:
            da_nhan_goi_end = True
            seq_end = so_thu_tu
            goi_ack = struct.pack('i i i I', 3, so_thu_tu, 0, 0)
            goi_ack_checksum = compute_checksum(goi_ack + b"")
            goi_ack = struct.pack('i i i I', 3, so_thu_tu, 0, goi_ack_checksum)
            socket_udp.sendto(goi_ack, dia_chi_client)

        # Thoát vòng lặp khi đã nhận gói END và gom đủ tất cả các gói dữ liệu
        if da_nhan_goi_end and so_thu_tu_mong_doi >= seq_end and len(kho_dem_goi_lech_thu_tu) == 0:
            break

    # Ghi toàn bộ dữ liệu ra stdout theo yêu cầu đề bài
    sys.stdout.buffer.write(du_lieu_nhan_duoc)
    sys.stdout.flush()
    socket_udp.close()

if __name__ == "__main__":
    if len(sys.argv) >= 3 and not sys.argv[1].isdigit():
        cong_ket_noi = int(sys.argv[2])
        kich_thuoc_cua_so = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    else:
        cong_ket_noi = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
        kich_thuoc_cua_so = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    chay_receiver(cong_ket_noi, kich_thuoc_cua_so)
