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
GATEWAY_NODE = "0002" 

if os.name == 'nt':  
    SERIAL_PORT = r'\\.\COM32' 
    RAM_DISK_CSV = 'topology_log.csv' 
    BACKUP_DIR = 'wsn_backup\\' 
else:                
    # Mặc định dùng /dev/serial0 cho chân GPIO 14/15 trên Pi
    SERIAL_PORT = '/dev/serial0'
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
        global_ser.reset_input_buffer()
        print(f"[Hệ Thống] Đã kết nối và khóa an toàn cổng {SERIAL_PORT}")
    except Exception as e:
        print(f"[LỖI CHÍ MẠNG] Không thể mở cổng Serial: {e}")
        os._exit(1)

# ==========================================
# LUỒNG 1: ĐỌC SERIAL (NHẬN DỮ LIỆU TỪ MẠNG)
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
        except Exception:
            time.sleep(1)

def send_uart_command(cmd):
    global global_ser
    if global_ser and global_ser.is_open:
        try:
            with serial_lock: 
                global_ser.write(f"{cmd}\r\n".encode())
                global_ser.flush() 
            print(f"[UART TX] Đã nã lệnh: {cmd}")
        except Exception as e:
            print(f"[UART TX] Lỗi truyền lệnh: {e}")
    else:
        print("[UART TX] Thất bại: Cổng Serial chưa mở.")

# ==========================================
# LUỒNG 2: XỬ LÝ LOGIC & VÒNG ĐỜI MẠNG
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
                nodes_to_commit.append(origin)
        
        for origin in nodes_to_commit:
            commit_topology_to_temp(origin)

        try:
            raw_line = uart_queue.get(timeout=0.1)
            match = re.search(TOPO_PATTERN, raw_line)
            if match and collecting_data:
                origin, seq, total, curr, count, grad, parent, n_str = match.groups()
                
                neighbors = re.findall(NEIGHBOR_PATTERN, n_str)
                parsed_nb = [{"addr": n[0], "rssi": int(n[1]), "grad": int(n[2])} for n in neighbors]

                if origin not in page_assembly or page_assembly[origin]['seq'] != int(seq):
                    page_assembly[origin] = {
                        'seq': int(seq), 'total': int(total), 'pages': {}, 
                        'grad': int(grad), 'parent': parent, 'ts': now
                    }
                
                page_assembly[origin]['pages'][int(curr)] = parsed_nb
                page_assembly[origin]['ts'] = now 

                if len(page_assembly[origin]['pages']) == int(total):
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
        "grad": data['grad'], "parent": data['parent'],
        "neighbors": all_neighbors, "is_partial": len(data['pages']) < data['total'] 
    }

def reconcile_master_graph():
    global Master_Graph, missed_count_dict, latest_graph_json
    with graph_lock:
        for node, data in current_cycle_data.items():
            missed_count_dict[node] = 0 
            color = "yellow" if data["is_partial"] else "green"
            Master_Graph.add_node(node, grad=data["grad"], color=color)
            Master_Graph.remove_edges_from([(u, v) for u, v in Master_Graph.edges if u == node])
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
                    Master_Graph.remove_node(node)
                    del missed_count_dict[node]
                else:
                    Master_Graph.nodes[node]['color'] = 'yellow'
    latest_graph_json = graph_to_json()

def save_to_csv_ramdisk():
    try:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(RAM_DISK_CSV, 'a', newline='') as f:
            writer = csv.writer(f)
            for origin, data in current_cycle_data.items():
                for nb in data["neighbors"]:
                    writer.writerow([ts, origin, data["grad"], data["parent"], nb["addr"], nb["rssi"]])
    except Exception as e:
        print(f"[CSV Error] {e}")

def backup_csv_thread():
    if not os.path.exists(BACKUP_DIR): os.makedirs(BACKUP_DIR)
    while True:
        time.sleep(BACKUP_INTERVAL)
        if os.path.exists(RAM_DISK_CSV):
            try:
                shutil.copy2(RAM_DISK_CSV, os.path.join(BACKUP_DIR, "topology_backup.csv"))
            except Exception: pass

# ==========================================
# LUỒNG 3: FASTAPI & WEBSOCKETS (ĐIỀU KHIỂN 2 CHIỀU)
# ==========================================
def graph_to_json():
    with graph_lock:
        nodes = []
        for n, d in Master_Graph.nodes(data=True):
            is_gw = (n == GATEWAY_NODE)
            nodes.append({
                "id": n, "label": f"Node {n}\n(Grad: {d.get('grad', 0 if is_gw else '?')})", 
                "color": "red" if is_gw else d.get('color', 'blue'),
                "level": 0 if is_gw else (int(d.get('grad', 3)) if str(d.get('grad')).isdigit() else 3)
            })
        edges = []
        for u, v, d in Master_Graph.edges(data=True):
            is_p = d.get('is_parent')
            edges.append({
                "from": u, "to": v, "label": f"{d.get('rssi')}dBm", 
                "width": 3 if is_p else 1, "dashes": not is_p, 
                "color": "red" if is_p else "gray", "physics": is_p
            })
    return json.dumps({"nodes": nodes, "edges": edges})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global latest_graph_json
    await websocket.accept()
    ws_clients.add(websocket)
    print(f"[WebSocket] Client connected. Total: {len(ws_clients)}")
    
    await websocket.send_text(graph_to_json())
    
    # NHIỆM VỤ 1: LẮNG NGHE LỆNH TỪ DASHBOARD
    async def listen_for_commands():
        try:
            while True:
                data = await websocket.receive_text()
                cmd = json.loads(data)
                action = cmd.get("action")
                target = cmd.get("target")
                
                if action == "toggle":
                    led_val = cmd.get("led") # 0-3
                    send_uart_command(f"mesh backprop {target} {led_val}")
                elif action == "identify":
                    send_uart_command(f"mesh attention {target}")
        except Exception: pass

    # NHIỆM VỤ 2: CẬP NHẬT ĐỒ THỊ LÊN DASHBOARD
    async def broadcast_graph():
        last_sent = ""
        try:
            while True:
                if latest_graph_json != last_sent and latest_graph_json != "":
                    await websocket.send_text(latest_graph_json)
                    last_sent = latest_graph_json
                await asyncio.sleep(0.5)
        except Exception: pass

    # CHẠY SONG SONG CẢ 2 NHIỆM VỤ TRÊN 1 KẾT NỐI
    await asyncio.gather(listen_for_commands(), broadcast_graph())
    ws_clients.remove(websocket)

if __name__ == "__main__":
    init_serial() 
    threading.Thread(target=uart_reader_thread, daemon=True).start()
    threading.Thread(target=data_processor_thread, daemon=True).start()
    threading.Thread(target=backup_csv_thread, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")