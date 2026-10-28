#24020632 Nguyen Trong Van Son
import socket
import sys
import time
import struct
from sender_receiver.util import compute_checksum

def chay_sender(dia_chi_ip, cong_ket_noi, kich_thuoc_cua_so):
    socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dia_chi_server = (dia_chi_ip, cong_ket_noi)
    
    du_lieu_nhap_vao = sys.stdin.buffer.read()
    gioi_han_kich_thuoc_goi = 1400
    
    danh_sach_phan_du_lieu = []
    for vi_tri in range(0, len(du_lieu_nhap_vao), gioi_han_kich_thuoc_goi):
        danh_sach_phan_du_lieu.append(du_lieu_nhap_vao[vi_tri:vi_tri + gioi_han_kich_thuoc_goi])
    
    tong_so_goi_tin = len(danh_sach_phan_du_lieu)
    
    def tao_goi_tin(loai_goi, so_thu_tu, du_lieu_gui=b""):
        do_dai = len(du_lieu_gui)
        khung_chua_co_checksum = struct.pack('i i i I', loai_goi, so_thu_tu, do_dai, 0)
        goi_chua_checksum = khung_chua_co_checksum + du_lieu_gui
        ma_checksum = compute_checksum(goi_chua_checksum)
        khung_hoan_chinh = struct.pack('i i i I', loai_goi, so_thu_tu, do_dai, ma_checksum)
        return khung_hoan_chinh + du_lieu_gui

    # Giai đoạn bắt tay: Gửi gói START (type = 0, seq_num = 0)
    goi_khoi_dau = tao_goi_tin(0, 0, b"")
    socket_udp.settimeout(0.5)
    
    da_ket_noi_thanh_cong = False
    while not da_ket_noi_thanh_cong:
        try:
            socket_udp.sendto(goi_khoi_dau, dia_chi_server)
            goi_tin_phan_hoi, _ = socket_udp.recvfrom(1024)
            if len(goi_tin_phan_hoi) >= 16:
                _, so_thu_tu_ack, _, _ = struct.unpack('i i i I', goi_tin_phan_hoi[:16])
                if so_thu_tu_ack == 1:
                    da_ket_noi_thanh_cong = True
                    break
        except socket.timeout:
            continue

    bien_duoi_cua_so = 0
    so_thu_tu_tiep_theo = 0
    
    kho_luu_goi_tin = {}
    trang_thai_ack = {} # Lưu trạng thái đã được ACK hay chưa cho từng gói
    for chi_so_goi, phan_du_lieu in enumerate(danh_sach_phan_du_lieu):
        seq = chi_so_goi + 1
        kho_luu_goi_tin[seq] = tao_goi_tin(2, seq, phan_du_lieu)
        trang_thai_ack[seq] = False

    thoi_gian_gui_goi_dau_tien = time.time()

    while bien_duoi_cua_so < tong_so_goi_tin:
        bien_tren_cua_so = min(bien_duoi_cua_so + kich_thuoc_cua_so, tong_so_goi_tin)
        
        # Gửi các gói chưa được gửi trong cửa sổ
        while so_thu_tu_tiep_theo < bien_tren_cua_so:
            seq_gui = so_thu_tu_tiep_theo + 1
            if not trang_thai_ack.get(seq_gui, False):
                socket_udp.sendto(kho_luu_goi_tin[seq_gui], dia_chi_server)
                thoi_gian_gui_goi_dau_tien = time.time()
            so_thu_tu_tiep_theo += 1

        try:
            socket_udp.settimeout(0.1)
            goi_tin_phan_hoi, _ = socket_udp.recvfrom(1024)
            if len(goi_tin_phan_hoi) >= 16:
                _, so_thu_tu_ack, _, _ = struct.unpack('i i i I', goi_tin_phan_hoi[:16])
                # RTP-opt: Nhận ACK độc lập cho từng gói
                if so_thu_tu_ack in trang_thai_ack:
                    if not trang_thai_ack[so_thu_tu_ack]:
                        trang_thai_ack[so_thu_tu_ack] = True
                        thoi_gian_gui_goi_dau_tien = time.time() # Reset timer khi có tiến triển ACK

                # Trượt cửa sổ nếu gói ở biên dưới đã được ACK
                while bien_duoi_cua_so < tong_so_goi_tin and trang_thai_ack.get(bien_duoi_cua_so + 1, False):
                    bien_duoi_cua_so += 1
        except socket.timeout:
            pass

        # Kiểm tra timeout 500ms cho toàn bộ cửa sổ hiện tại (chỉ truyền lại các gói chưa ACK)
        if time.time() - thoi_gian_gui_goi_dau_tien > 0.5:
            for seq in range(bien_duoi_cua_so + 1, bien_tren_cua_so + 1):
                if not trang_thai_ack.get(seq, False):
                    socket_udp.sendto(kho_luu_goi_tin[seq], dia_chi_server)
            thoi_gian_gui_goi_dau_tien = time.time()

    # Giai đoạn kết thúc: Gửi gói END (type = 1)
    goi_ket_thuc = tao_goi_tin(1, tong_so_goi_tin + 1, b"")
    da_nhan_ack_ket_thuc = False
    thoi_gian_het_han = time.time() + 0.5
    
    while not da_nhan_ack_ket_thuc and time.time() < thoi_gian_het_han:
        try:
            socket_udp.sendto(goi_ket_thuc, dia_chi_server)
            socket_udp.settimeout(0.1)
            goi_tin_phan_hoi, _ = socket_udp.recvfrom(1024)
            if len(goi_tin_phan_hoi) >= 16:
                da_nhan_ack_ket_thuc = True
                break
        except socket.timeout:
            continue

    socket_udp.close()

if __name__ == "__main__":
    dia_chi_ip = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
    cong_ket_noi = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
    kich_thuoc_cua_so = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    chay_sender(dia_chi_ip, cong_ket_noi, kich_thuoc_cua_so)
