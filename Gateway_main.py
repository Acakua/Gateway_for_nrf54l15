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
from fastapi.responses import FileResponse # THÊM ĐỂ LÀM WEB SERVER
import uvicorn
import asyncio

# ==========================================
# CẤU HÌNH HỆ THỐNG & ĐỊNH DANH PHIÊN (SESSION)
# ==========================================
BAUD_RATE = 115200
POLLING_INTERVAL = 30 
COLLECTION_WINDOW = 15 
PAGE_TIMEOUT = 4 
BACKUP_INTERVAL = 300 
GATEWAY_NODE = "0002" 

# Tạo định danh duy nhất cho mỗi lần khởi động: ví dụ 20260310_174501
SESSION_ID = time.strftime("%Y%m%d_%H%M%S")

if os.name == 'nt':  
    SERIAL_PORT = r'\\.\COM32' 
    # File log chính trong RAM Disk (Session-based)
    RAM_DISK_CSV = f'topology_log_{SESSION_ID}.csv' 
    BACKUP_DIR = 'wsn_backup\\' 
else:                
    SERIAL_PORT = '/dev/serial0'
    RAM_DISK_CSV = f'/dev/shm/topology_log_{SESSION_ID}.csv' 
    BACKUP_DIR = '/home/pi/wsn_backup/'

TOPO_PATTERN = r"\$\[TOPO\],([0-9A-F]{4}),(\d+),(\d+),(\d+),(\d+),(\d+),([0-9A-F]{4}),(.*)"
NEIGHBOR_PATTERN = r"\[([0-9A-F]{4}),(-?\d+),(\d+)\]"

# ==========================================
# BIẾN TOÀN CỤC & KHÓA AN TOÀN
# ==========================================
uart_queue = queue.Queue() 
ws_clients = set() 
Master_Graph = nx.DiGraph()
latest_graph_json = ""
missed_count_dict = {}

graph_lock = threading.Lock()
serial_lock = threading.Lock() # Universal Lock bảo vệ UART

app = FastAPI()
global_ser = None

def init_serial():
    global global_ser
    try:
        global_ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=0.1)
        global_ser.reset_input_buffer()
        global_ser.reset_output_buffer()
        print(f"--- SESSION STARTED: {SESSION_ID} ---")
        print(f"[Hệ Thống] Đã kết nối cổng: {SERIAL_PORT}")
    except Exception as e:
        print(f"[LỖI CHÍ MẠNG] Serial Error: {e}")
        os._exit(1)

# ==========================================
# LUỒNG 1: ĐỌC SERIAL (CÓ KHÓA AN TOÀN)
# ==========================================
def uart_reader_thread():
    global global_ser
    print("[Luồng 1] Đang túc trực lắng nghe dữ liệu...")
    while True:
        try:
            if global_ser and global_ser.is_open:
                with serial_lock:
                    if global_ser.in_waiting > 0:
                        line = global_ser.readline().decode('utf-8', errors='ignore').strip()
                        if line:
                            print(f"[RAW UART] {line}")
                            if "$[TOPO]" in line:
                                uart_queue.put(line)
            time.sleep(0.01)
        except Exception:
            time.sleep(1)

def send_uart_command(cmd):
    global global_ser
    if global_ser and global_ser.is_open:
        try:
            with serial_lock:
                global_ser.write(f"{cmd}\r\n".encode())
                global_ser.flush() 
                time.sleep(0.05) 
            print(f"[UART TX] Đã nã lệnh: {cmd}")
        except Exception as e:
            print(f"[UART TX] Lỗi: {e}")

# ==========================================
# LUỒNG 2: XỬ LÝ LOGIC & SAO LƯU THÔNG MINH
# ==========================================
def data_processor_thread():
    print("[Luồng 2] Khởi động Xử lý Dữ liệu")
    last_polling_time = time.time()
    collecting_data = False
    global current_cycle_data, page_assembly
    current_cycle_data = {}
    page_assembly = {}

    while True:
        now = time.time()
        if now - last_polling_time >= POLLING_INTERVAL:
            print("\n=======================================")
            send_uart_command("mesh topo_req")
            current_cycle_data.clear()
            page_assembly.clear()
            collecting_data = True
            last_polling_time = now

        if collecting_data and (now - last_polling_time >= COLLECTION_WINDOW):
            collecting_data = False
            reconcile_master_graph()
            save_to_csv_ramdisk()

        try:
            raw_line = uart_queue.get(timeout=0.1)
            match = re.search(TOPO_PATTERN, raw_line)
            if match and collecting_data:
                origin, seq, total, curr, count, grad, parent, n_str = match.groups()
                neighbors = re.findall(NEIGHBOR_PATTERN, n_str)
                parsed_nb = [{"addr": n[0], "rssi": int(n[1]), "grad": int(n[2])} for n in neighbors]
                
                if origin not in page_assembly or page_assembly[origin]['seq'] != int(seq):
                    page_assembly[origin] = {'seq': int(seq), 'total': int(total), 'pages': {}, 'grad': int(grad), 'parent': parent, 'ts': now}
                
                page_assembly[origin]['pages'][int(curr)] = parsed_nb
                page_assembly[origin]['ts'] = now 
                if len(page_assembly[origin]['pages']) == int(total):
                    commit_topology_to_temp(origin)
        except queue.Empty: pass

def commit_topology_to_temp(origin):
    if origin not in page_assembly: return
    data = page_assembly.pop(origin) 
    all_neighbors = []
    for page_num in sorted(data['pages'].keys()):
        all_neighbors.extend(data['pages'][page_num])
    current_cycle_data[origin] = {"grad": data['grad'], "parent": data['parent'], "neighbors": all_neighbors, "is_partial": len(data['pages']) < data['total']}

def reconcile_master_graph():
    global Master_Graph, latest_graph_json, missed_count_dict
    with graph_lock:
        for node, data in current_cycle_data.items():
            missed_count_dict[node] = 0
            Master_Graph.add_node(node, grad=data["grad"], color="green")
            Master_Graph.remove_edges_from([(u, v) for u, v in Master_Graph.edges if u == node])
            for nb in data["neighbors"]:
                Master_Graph.add_edge(node, nb["addr"], rssi=nb["rssi"], is_parent=(nb["addr"] == data["parent"]))
        
        # Xử lý node mất tích
        for node in list(Master_Graph.nodes):
            if node == GATEWAY_NODE: continue
            if node not in current_cycle_data:
                missed_count_dict[node] = missed_count_dict.get(node, 0) + 1
                if missed_count_dict[node] >= 3:
                    Master_Graph.remove_node(node)
                else:
                    Master_Graph.nodes[node]['color'] = 'yellow'
    latest_graph_json = graph_to_json()

def save_to_csv_ramdisk():
    try:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        file_exists = os.path.isfile(RAM_DISK_CSV)
        with open(RAM_DISK_CSV, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Timestamp", "Origin", "Grad", "Parent", "Neighbor", "RSSI"])
            for origin, data in current_cycle_data.items():
                for nb in data["neighbors"]:
                    writer.writerow([ts, origin, data["grad"], data["parent"], nb["addr"], nb["rssi"]])
    except Exception as e:
        print(f"[CSV Error] {e}")

def backup_csv_thread():
    if not os.path.exists(BACKUP_DIR): os.makedirs(BACKUP_DIR)
    print(f"[Backup] Luồng sao lưu snapshot ({BACKUP_INTERVAL}s) đã sẵn sàng.")
    while True:
        time.sleep(BACKUP_INTERVAL)
        if os.path.exists(RAM_DISK_CSV):
            # Mỗi bản backup có tên riêng: topology_snapshot_20260310_174501_175001.csv
            ts_now = time.strftime("%H%M%S")
            backup_filename = f"topology_snapshot_{SESSION_ID}_{ts_now}.csv"
            try:
                shutil.copy2(RAM_DISK_CSV, os.path.join(BACKUP_DIR, backup_filename))
                print(f"[Backup] Đã lưu snapshot mới: {backup_filename}")
            except Exception: pass

# ==========================================
# LUỒNG 3: FASTAPI & WEBSOCKETS CÓ WEB SERVER
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

# --- ROUTE MỚI: TRẢ VỀ FILE GIAO DIỆN ---
@app.get("/")
async def get_dashboard():
    return FileResponse("index.html")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    await websocket.send_text(latest_graph_json if latest_graph_json else graph_to_json())
    
    async def listen_for_commands():
        try:
            while True:
                data = await websocket.receive_text()
                cmd = json.loads(data)
                target, action, led = cmd.get("target"), cmd.get("action"), cmd.get("led")
                if action == "toggle":
                    send_uart_command(f"mesh backprop {target} {led}")
                elif action == "identify":
                    send_uart_command(f"mesh attention {target}")
        except: pass

    async def broadcast_graph():
        last_sent = ""
        try:
            while True:
                if latest_graph_json != last_sent and latest_graph_json != "":
                    await websocket.send_text(latest_graph_json)
                    last_sent = latest_graph_json
                await asyncio.sleep(0.5)
        except: pass

    await asyncio.gather(listen_for_commands(), broadcast_graph())
    ws_clients.remove(websocket)

if __name__ == "__main__":
    init_serial() 
    threading.Thread(target=uart_reader_thread, daemon=True).start()
    threading.Thread(target=data_processor_thread, daemon=True).start()
    threading.Thread(target=backup_csv_thread, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")