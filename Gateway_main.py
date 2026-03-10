import serial
import re
import threading
import queue
import time
import json
import csv
import shutil
import os
import networkx as nx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn
import asyncio

# ==========================================
# CẤU HÌNH HỆ THỐNG
# ==========================================
BAUD_RATE = 115200
POLLING_INTERVAL = 30 # Giây
COLLECTION_WINDOW = 15 # Giây
PAGE_TIMEOUT = 4 # Giây
BACKUP_INTERVAL = 300 # 5 phút
GATEWAY_NODE = "0002" # <--- Cập nhật theo log mới nhất của bạn

if os.name == 'nt':  
    SERIAL_PORT = r'\\.\COM32' 
    RAM_DISK_CSV = 'topology_log.csv' 
    BACKUP_DIR = 'wsn_backup\\' 
else:                
    SERIAL_PORT = '/dev/ttyUSB0'
    RAM_DISK_CSV = '/dev/shm/topology_log.csv'
    BACKUP_DIR = '/home/pi/wsn_backup/'

TOPO_PATTERN = r"\$\[TOPO\],([0-9A-F]{4}),(\d+),(\d+),(\d+),(\d+),(\d+),([0-9A-F]{4}),(.*)"
NEIGHBOR_PATTERN = r"\[([0-9A-F]{4}),(-?\d+),(\d+)\]"

# ==========================================
# BIẾN TOÀN CỤC & HÀNG ĐỢI
# ==========================================
uart_queue = queue.Queue() 
ws_clients = set() 

Master_Graph = nx.DiGraph()
missed_count_dict = {} 
current_cycle_data = {} 
page_assembly = {} 
latest_graph_json = ""

graph_lock = threading.Lock()
app = FastAPI()

global_ser = None
serial_lock = threading.Lock()

def init_serial():
    global global_ser
    try:
        global_ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print(f"[Hệ Thống] Đã kết nối và khóa an toàn cổng {SERIAL_PORT}")
    except Exception as e:
        print(f"[LỖI CHÍ MẠNG] Không thể mở cổng Serial: {e}")
        os._exit(1)

# ==========================================
# LUỒNG 1: ĐỌC SERIAL
# ==========================================
def uart_reader_thread():
    global global_ser
    print("[Luồng 1] Đang túc trực lắng nghe dữ liệu...")
    
    while True:
        try:
            if global_ser and global_ser.in_waiting > 0:
                line = global_ser.readline().decode('utf-8', errors='ignore').strip()
                
                if line: 
                    print(f"[RAW UART] {line}")
                    
                if "$[TOPO]" in line:
                    uart_queue.put(line)
        except Exception as e:
            time.sleep(1)

def send_uart_command(cmd):
    global global_ser
    if global_ser and global_ser.is_open:
        try:
            with serial_lock: 
                global_ser.write(f"{cmd}\r\n".encode())
                global_ser.flush() 
            print(f"[Luồng 2] Đã gửi lệnh Broadcast: {cmd}")
        except Exception as e:
            print(f"[Luồng 2] Lỗi truyền dữ liệu: {e}")
    else:
        print("[Luồng 2] Không thể gửi lệnh: Cổng Serial chưa mở.")

# ==========================================
# LUỒNG 2: XỬ LÝ LOGIC, LẮP RÁP & VÒNG ĐỜI MẠNG
# ==========================================
def data_processor_thread():
    global current_cycle_data
    print("[Luồng 2] Khởi động Xử lý Dữ liệu & Polling")
    
    last_polling_time = time.time()
    collecting_data = False

    while True:
        now = time.time()

        if now - last_polling_time >= POLLING_INTERVAL:
            print("\n=======================================")
            print("[Luồng 2] BẮT ĐẦU CHU KỲ POLLING MỚI")
            send_uart_command("mesh topo_req")
            current_cycle_data.clear()
            page_assembly.clear()
            collecting_data = True
            last_polling_time = now

        if collecting_data and (now - last_polling_time >= COLLECTION_WINDOW):
            collecting_data = False
            print("[Luồng 2] HẾT THỜI GIAN THU THẬP. CHỐT SỔ ĐỒ THỊ!")
            reconcile_master_graph()
            save_to_csv_ramdisk()

        nodes_to_commit = []
        for origin, data in list(page_assembly.items()):
            if now - data['ts'] > PAGE_TIMEOUT:
                print(f"[Luồng 2] Node {origin} Timeout trang! Tiến hành Partial Commit.")
                nodes_to_commit.append(origin)
        
        for origin in nodes_to_commit:
            commit_topology_to_temp(origin)

        try:
            raw_line = uart_queue.get(timeout=0.1)
            match = re.search(TOPO_PATTERN, raw_line)
            if match and collecting_data:
                origin = match.group(1)
                seq = int(match.group(2))
                total_pages = int(match.group(3))
                current_page = int(match.group(4))
                count = int(match.group(5))
                grad = int(match.group(6))
                parent = match.group(7)
                neighbors_str = match.group(8)

                neighbors = re.findall(NEIGHBOR_PATTERN, neighbors_str)
                parsed_neighbors = [{"addr": n[0], "rssi": int(n[1]), "grad": int(n[2])} for n in neighbors]

                if origin not in page_assembly or page_assembly[origin]['seq'] != seq:
                    page_assembly[origin] = {
                        'seq': seq, 'total': total_pages, 'pages': {}, 
                        'grad': grad, 'parent': parent, 'ts': now
                    }
                
                page_assembly[origin]['pages'][current_page] = parsed_neighbors
                page_assembly[origin]['ts'] = now 
                print(f"[Luồng 2] Đang lắp ráp: Nhận trang {current_page}/{total_pages} từ Node {origin}")

                if len(page_assembly[origin]['pages']) == total_pages:
                    print(f"[Luồng 2] Đã nhận ĐỦ {total_pages} trang từ Node {origin}. Commit Thành công!")
                    commit_topology_to_temp(origin)

        except queue.Empty:
            continue

def commit_topology_to_temp(origin):
    if origin not in page_assembly: return
    
    data = page_assembly.pop(origin) 
    all_neighbors = []
    for page_num in sorted(data['pages'].keys()):
        all_neighbors.extend(data['pages'][page_num])
    
    current_cycle_data[origin] = {
        "grad": data['grad'],
        "parent": data['parent'],
        "neighbors": all_neighbors,
        "is_partial": len(data['pages']) < data['total'] 
    }

def reconcile_master_graph():
    global Master_Graph, missed_count_dict, latest_graph_json
    
    with graph_lock:
        for node, data in current_cycle_data.items():
            missed_count_dict[node] = 0 
            color = "yellow" if data["is_partial"] else "green"
            
            Master_Graph.add_node(node, grad=data["grad"], color=color)
            
            edges_to_remove = [(u, v) for u, v in Master_Graph.edges if u == node]
            Master_Graph.remove_edges_from(edges_to_remove)

            for nb in data["neighbors"]:
                is_parent = (nb["addr"] == data["parent"])
                Master_Graph.add_edge(node, nb["addr"], rssi=nb["rssi"], is_parent=is_parent)

        existing_nodes = list(Master_Graph.nodes)
        for node in existing_nodes:
            if node == GATEWAY_NODE: continue 

            if node not in current_cycle_data:
                miss_count = missed_count_dict.get(node, 0) + 1
                missed_count_dict[node] = miss_count

                if miss_count >= 3:
                    print(f"[Luồng 2] Node {node} miss 3 lần. XÓA SỔ khỏi mạng!")
                    Master_Graph.remove_node(node)
                    del missed_count_dict[node]
                else:
                    print(f"[Luồng 2] Node {node} miss {miss_count} lần. Chuyển sang Warning (Vàng).")
                    Master_Graph.nodes[node]['color'] = 'yellow'

    latest_graph_json = graph_to_json()

def save_to_csv_ramdisk():
    file_exists = os.path.isfile(RAM_DISK_CSV)
    try:
        with open(RAM_DISK_CSV, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Timestamp", "Origin", "Grad", "Parent", "Neighbor_Addr", "RSSI"])
            
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            for origin, data in current_cycle_data.items():
                for nb in data["neighbors"]:
                    writer.writerow([ts, origin, data["grad"], data["parent"], nb["addr"], nb["rssi"]])
    except Exception as e:
        print(f"[Lỗi Ghi CSV] {e}")

# ==========================================
# LUỒNG 4: SAO LƯU CSV 
# ==========================================
def backup_csv_thread():
    print(f"[Backup] Khởi động sao lưu định kỳ ({BACKUP_INTERVAL}s)")
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    
    while True:
        time.sleep(BACKUP_INTERVAL)
        if os.path.exists(RAM_DISK_CSV):
            backup_path = os.path.join(BACKUP_DIR, "topology_log_backup.csv")
            try:
                shutil.copy2(RAM_DISK_CSV, backup_path)
                print(f"[Backup] Đã sao lưu dữ liệu an toàn: {backup_path}")
            except Exception as e:
                print(f"[Backup] Lỗi sao lưu: {e}")

# ==========================================
# LUỒNG 3: FASTAPI & WEBSOCKETS (MAIN THREAD)
# ==========================================
def graph_to_json():
    with graph_lock:
        nodes = []
        for n, d in Master_Graph.nodes(data=True):
            # Tính toán phân cấp: Gateway ép cứng level 0, các node khác lấy grad
            if n == GATEWAY_NODE:
                level = 0
                display_grad = "0"
                color = "red" # Gateway cho màu đỏ cho nổi bật
            else:
                raw_grad = d.get('grad', '?')
                level = int(raw_grad) if str(raw_grad).isdigit() else 3 # Node rác ném ra vòng ngoài cùng
                display_grad = str(raw_grad)
                color = d.get('color', 'blue')

            nodes.append({
                "id": n, 
                "label": f"Node {n}\n(Grad: {display_grad})", 
                "color": color,
                "level": level # Gắn thẻ cho Vis.js phân lớp
            })

        edges = []
        for u, v, d in Master_Graph.edges(data=True):
            width = 3 if d.get('is_parent') else 1
            dashes = False if d.get('is_parent') else True
            color = "red" if d.get('is_parent') else "gray"
            # Vô hiệu hóa lực kéo vật lý của đường nét đứt để không kéo lệch đồ thị
            physics = True if d.get('is_parent') else False
            
            edges.append({
                "from": u, "to": v, 
                "label": f"{d.get('rssi')}dBm", 
                "width": width, "dashes": dashes, 
                "color": color, "physics": physics
            })
    
    return json.dumps({"nodes": nodes, "edges": edges})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global latest_graph_json
    await websocket.accept()
    ws_clients.add(websocket)
    print(f"[WebSocket] Có client mới kết nối. (Tổng: {len(ws_clients)})")
    
    await websocket.send_text(graph_to_json())
    
    last_sent_json = ""
    try:
        while True:
            if latest_graph_json != last_sent_json and latest_graph_json != "":
                await websocket.send_text(latest_graph_json)
                last_sent_json = latest_graph_json
            
            await asyncio.sleep(0.5) 
    except WebSocketDisconnect:
        ws_clients.remove(websocket)
        print("[WebSocket] Client ngắt kết nối.")

if __name__ == "__main__":
    init_serial() 
    
    threading.Thread(target=uart_reader_thread, daemon=True).start()
    threading.Thread(target=data_processor_thread, daemon=True).start()
    threading.Thread(target=backup_csv_thread, daemon=True).start()

    print("\n[HỆ THỐNG] Gateway WSN đã sẵn sàng trên Windows!")
    print("[HỆ THỐNG] Mở trình duyệt, truy cập Frontend để xem đồ thị")
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")