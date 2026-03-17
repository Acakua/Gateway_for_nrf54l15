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
from fastapi.responses import FileResponse
import uvicorn
import asyncio
import datetime
import math

# ==========================================
# CẤU HÌNH HỆ THỐNG & ĐỊNH DANH PHIÊN (SESSION)
# ==========================================
BAUD_RATE = 115200
POLLING_INTERVAL = 60 
COLLECTION_WINDOW = 30 
PAGE_TIMEOUT = 4 
BACKUP_INTERVAL = 300 
GATEWAY_NODE = "0002" 

SESSION_ID = time.strftime("%Y%m%d_%H%M%S") + "_PURE"

if os.name == 'nt':  
    SERIAL_PORT = r'\\.\COM32' 
    RAM_DISK_CSV = f'topology_log_{SESSION_ID}.csv'
    PERFORMANCE_CSV = f'performance_log_{SESSION_ID}.csv'
    BACKUP_DIR = 'wsn_backup\\' 
else:                
    SERIAL_PORT = '/dev/serial0'
    RAM_DISK_CSV = f'/dev/shm/topology_log_{SESSION_ID}.csv'
    PERFORMANCE_CSV = f'/dev/shm/performance_log_{SESSION_ID}.csv'
    BACKUP_DIR = '/home/pi/wsn_backup/'

# Format: $[TOPO],Origin,Seq,TotalPg,CurPg,Count,Grad,Parent,Drp(u16),FwdR(u16),Uptime(u32),TotalSent(u32),[neighbors]
TOPO_PATTERN = r"\$\[TOPO\],([0-9A-F]{4}),(\d+),(\d+),(\d+),(\d+),(\d+),([0-9A-F]{4}),(\d+),(\d+),(\d+),(\d+),(.*)"
NEIGHBOR_PATTERN = r"\[([0-9A-F]{4}),(-?\d+),(\d+),(\d+)\]"

# ==========================================
# BIẾN TOÀN CỤC & KHÓA AN TOÀN
# ==========================================
uart_queue = queue.Queue() 
ws_clients = set() 
Master_Graph = nx.DiGraph()      
latest_graph_json = ""
missed_count_dict = {}
current_cycle_data = {}          
page_assembly = {}               

# --- DATA LOGGING CACHE (Ported from sink_logger.py) ---
rx_stats = {}           # { src_hex: set(seq) }
latency_tracker = {}    # { src_hex: rtt_ms }
node_report_cache = {}  # { src_hex: { 'beacon_tx': val, 'hb_tx': val, ... } }
latency_sync_cache = {} # { (src_hex, seq): timestamp }
reported_nodes = set()  

graph_lock = threading.Lock()
serial_lock = threading.Lock() 

app = FastAPI()
global_ser = None

def init_serial():
    global global_ser
    try:
        global_ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=0.1)
        global_ser.reset_input_buffer()
        global_ser.reset_output_buffer()
        print(f"--- PURE GRADIENT SESSION STARTED: {SESSION_ID} ---")
        print(f"[Hệ Thống] Đã kết nối cổng: {SERIAL_PORT}")
        
        # [SDN FAIL-SAFE] Broadcast SDN Reset to clear any AI-directed routes
        # This ensures the network reverts to pure Gradient immediately.
        time.sleep(1) # Wait for serial to stabilize
        send_uart_command("mesh sdn_reset") 
    except Exception as e:
        print(f"[LỖI CHÍ MẠNG] Serial Error: {e}")
        os._exit(1)

# ==========================================
# TÍNH TOÁN ROUTING COST PER-LINK
# ==========================================
def compute_routing_cost_per_link(grad, nb_rssi, nb_link_up_s, drop_rate, pin):
    """
    Tính Routing_Cost bằng Heuristic (Thuần Gradient).
    """
    hop_cost = grad * 10.0
    signal_cost = max(0.0, (-nb_rssi - 70.0)) * 2.5
    reliability_cost = min(400.0, pow(drop_rate, 1.5)) if drop_rate > 0 else 0.0
    if pin < 20.0:
        battery_cost = 200.0
    elif pin < 60.0:
        battery_cost = (60.0 - pin) * 1.5
    else:
        battery_cost = 0.0
    
    stability_cost = 100.0 * math.exp(-nb_link_up_s / 300.0)
    total = hop_cost + signal_cost + reliability_cost + battery_cost + stability_cost
    return round(total, 2)

# --- HELPER FUNCTIONS FOR LOG PARSING ---
def safe_int_convert(val):
    try:
        if val is None: return 0
        s = str(val).strip()
        if not s: return 0
        if s.lower().startswith('0x'): return int(s, 16)
        if '.' in s: s = s.split('.')[0]
        return int(s)
    except: return 0

def safe_float_convert(val):
    try:
        if val is None: return 0.0
        s = str(val).strip()
        if not s: return 0.0
        return float(s)
    except: return 0.0

def write_performance_log(row_data):
    try:
        file_exists = os.path.isfile(PERFORMANCE_CSV)
        with open(PERFORMANCE_CSV, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "Timestamp", "Type", "SourceAddr", "SenderAddr", 
                    "Seq_or_TxCount", "HopCount", "Latency_ms", "PathMinRSSI", 
                    "BeaconTx", "HeartbeatTx", "RouteChanges", "FwdCount", 
                    "Rx_Unique_Count", "Remote_Rx_Count", "PDR_Percent"
                ])
            writer.writerow(row_data)
    except Exception as e:
        print(f"[PERF LOG ERROR] {e}")

def handle_csv_log(line):
    global rx_stats, latency_tracker, node_report_cache, latency_sync_cache, reported_nodes
    now_ts = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
    try:
        if "CSV_LOG," not in line: return
        raw_data = line.split("CSV_LOG,")[1]
        parts = raw_data.split(',')
        if not parts: return
        log_type = parts[0].strip()

        row = [now_ts, log_type, "", "", "", "", "", "", "", "", "", "", "", "", ""]

        if log_type == "DATA":
            # [FIX] Changed from 8 to 7 because the printk payload has 7 fields after CSV_LOG,
            if len(parts) >= 7:
                src_val = safe_int_convert(parts[1])
                sender_val = safe_int_convert(parts[2])
                seq_val = safe_int_convert(parts[3])
                # Sanitize numeric fields in case of UART mangling (strip non-numeric except minus sign)
                def sanitize_num(s):
                    return re.sub(r'[^0-9\-]', '', str(s))

                hops = sanitize_num(parts[4])
                lat = sanitize_num(parts[5])
                rssi = sanitize_num(parts[6]) # Actually parts[6] is PathMinRSSI or Delay
                
                # Re-check indexing based on gradient_srv.c: 
                # printk("CSV_LOG,DATA,0x%04x,0x%04x,%d,%d,0,%d\n", src, sender, seq, hops, path_min_rssi)
                # Parts: [0:DATA, 1:Src, 2:Sender, 3:Seq, 4:Hops, 5:DummyDelay, 6:PathMinRSSI]
                
                src_hex = f"0x{src_val:04x}"
                if src_hex not in rx_stats: rx_stats[src_hex] = set()
                rx_stats[src_hex].add(seq_val)
                latency_sync_cache[(src_hex, seq_val)] = time.time()
                
                row[2] = src_hex
                row[3] = f"0x{sender_val:04x}"
                row[4] = seq_val
                row[5] = hops
                row[6] = lat
                row[7] = rssi
                write_performance_log(row)

        elif log_type == "RTT_DATA":
            if len(parts) >= 4:
                src_hex = f"0x{safe_int_convert(parts[1]):04x}"
                rtt_val = safe_float_convert(parts[3])
                latency_tracker[src_hex] = rtt_val

        elif log_type == "HEARTBEAT":
            if len(parts) >= 5:
                src_hex = f"0x{safe_int_convert(parts[1]):04x}"
                sender_hex = f"0x{safe_int_convert(parts[2]):04x}"
                latency_tracker[src_hex] = safe_float_convert(parts[4])
                
                row[2] = src_hex
                row[3] = sender_hex
                row[5] = parts[3]
                row[6] = parts[4]
                write_performance_log(row)

        elif log_type == "REPORT":
            if len(parts) >= 7:
                src_hex = f"0x{safe_int_convert(parts[1]):04x}"
                tx_count = safe_int_convert(parts[2])
                beacon = safe_int_convert(parts[3])
                hb = safe_int_convert(parts[4])
                r_chg = safe_int_convert(parts[5])
                fwd = safe_int_convert(parts[6])
                remote_rx = safe_int_convert(parts[7]) if len(parts) > 7 else 0
                
                rx_unique = len(rx_stats.get(src_hex, set()))
                pdr = (rx_unique / tx_count * 100.0) if tx_count > 0 else 0.0

                node_report_cache[src_hex] = {
                    'tx_count': tx_count,
                    'beacon_tx': beacon,
                    'hb_tx': hb,
                    'route_changes': r_chg,
                    'fwd_count': fwd,
                    'remote_rx': remote_rx
                }
                
                row[2] = src_hex
                row[4] = tx_count
                row[8] = beacon
                row[9] = hb
                row[10] = r_chg
                row[11] = fwd
                row[12] = rx_unique
                row[13] = remote_rx
                row[14] = f"{pdr:.2f}"
                write_performance_log(row)

        elif log_type == "EVENT":
            if len(parts) >= 2:
                event_name = parts[1].strip()
                msg = parts[2].strip() if len(parts) > 2 else ""
                if "TEST_START" in event_name:
                    rx_stats.clear()
                    latency_tracker.clear()
                    latency_sync_cache.clear()
                    reported_nodes.clear()
                    print(f"\n[LOGGER] --- NEW TEST SESSION: {event_name} (Caches Cleared) ---")
                
                # Log event summary to performance log
                row[2] = event_name
                row[3] = msg
                write_performance_log(row)

    except Exception as e:
        print(f"[LOGGER ERROR] Parsing line {line} -> {e}")

# ==========================================
# KHUNG SƯỜN TÍNH TOÁN NĂNG LƯỢNG
# ==========================================
def estimate_battery(uptime_s, tx_count):
    W_idle = 0.00005  
    W_tx = 0.002      
    battery_left = 100.0 - (uptime_s * W_idle + tx_count * W_tx)
    return round(max(0.0, min(100.0, battery_left)), 1)

# ==========================================
# LUỒNG 1: ĐỌC SERIAL
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
                            if "$[TOPO]" in line or "CSV_LOG" in line:
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
# LUỒNG 2: XỬ LÝ LOGIC & SAO LƯU
# ==========================================
def data_processor_thread():
    global current_cycle_data, page_assembly
    print("[Luồng 2] Khởi động Xử lý Dữ liệu (Pure Gradient Mode)")
    last_polling_time = time.time()
    collecting_data = False
    current_cycle_data = {}
    page_assembly = {}

    while True:
        now = time.time()
        if now - last_polling_time >= POLLING_INTERVAL:
            print("\n=======================================")
            send_uart_command("mesh topo_req 0")
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
                origin, seq, total, curr, count, grad, parent, drp, fwdr, uptime, total_sent, n_str = match.groups()
                neighbors = re.findall(NEIGHBOR_PATTERN, n_str)
                parsed_nb = [{"addr": n[0], "rssi": int(n[1]), "grad": int(n[2]), "link_uptime": int(n[3])} for n in neighbors]
                
                if origin not in page_assembly or page_assembly[origin]['seq'] != int(seq):
                    page_assembly[origin] = {
                        'seq': int(seq), 'total': int(total), 'pages': {}, 
                        'grad': int(grad), 'parent': parent, 'ts': now,
                        'drp': int(drp), 'fwdr': int(fwdr), 
                        'uptime': int(uptime), 'total_sent': int(total_sent)
                    }
                
                page_assembly[origin]['pages'][int(curr)] = parsed_nb
                page_assembly[origin]['ts'] = now 
                if len(page_assembly[origin]['pages']) == int(total):
                    commit_topology_to_temp(origin)
            elif "CSV_LOG" in raw_line and collecting_data:
                handle_csv_log(raw_line)
        except queue.Empty: pass

def commit_topology_to_temp(origin):
    if origin not in page_assembly: return
    data = page_assembly.pop(origin) 
    all_neighbors = []
    for page_num in sorted(data['pages'].keys()):
        all_neighbors.extend(data['pages'][page_num])
        
    pin_est = estimate_battery(data['uptime'], data['total_sent'])
    total_processed = data['drp'] + data['fwdr']
    drop_rate = round((data['drp'] / total_processed * 100.0), 1) if total_processed > 0 else 0.0
    
    current_cycle_data[origin] = {
        "grad": data['grad'], "parent": data['parent'], "neighbors": all_neighbors, 
        "drp": data['drp'], "fwdr": data['fwdr'], "drop_rate": drop_rate, "pin": pin_est
    }

def reconcile_master_graph():
    global Master_Graph, latest_graph_json, missed_count_dict
    with graph_lock:
        for node, data in current_cycle_data.items():
            missed_count_dict[node] = 0
            Master_Graph.add_node(node, grad=data["grad"], color="green", drp=data["drp"], fwdr=data["fwdr"], drop_rate=data["drop_rate"], pin=data["pin"])
            Master_Graph.remove_edges_from([(u, v) for u, v in Master_Graph.edges if u == node])
            for nb in data["neighbors"]:
                cost = compute_routing_cost_per_link(data["grad"], nb["rssi"], nb["link_uptime"], data["drop_rate"], data["pin"])
                Master_Graph.add_edge(node, nb["addr"], rssi=nb["rssi"], is_parent=(nb["addr"] == data["parent"]), link_uptime=nb["link_uptime"], cost=cost)
            
            parent = data["parent"]
            if parent != "0000" and parent != "ffff":
                if not Master_Graph.has_edge(node, parent):
                    Master_Graph.add_edge(node, parent, rssi=-99, is_parent=True, link_uptime=0, cost=999, is_virtual=True)
                else:
                    Master_Graph[node][parent]['is_parent'] = True
        
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
                writer.writerow(["Timestamp", "Origin", "Grad", "Parent", "Neighbor", "RSSI", "Link_UP(s)", "Drop_Count", "Fwd_Count", "Drop_Rate(%)", "Pin(%)", "Routing_Cost"])
            for origin, data in current_cycle_data.items():
                safe_origin, safe_parent = f'="{origin}"', f'="{data["parent"]}"'
                for nb in data["neighbors"]:
                    cost = compute_routing_cost_per_link(data["grad"], nb["rssi"], nb["link_uptime"], data["drop_rate"], data["pin"])
                    writer.writerow([ts, safe_origin, data["grad"], safe_parent, f'="{nb["addr"]}"', nb["rssi"], nb["link_uptime"], data["drp"], data["fwdr"], data["drop_rate"], data["pin"], cost])
    except: pass

def backup_csv_thread():
    if not os.path.exists(BACKUP_DIR): os.makedirs(BACKUP_DIR)
    while True:
        time.sleep(BACKUP_INTERVAL)
        if os.path.exists(RAM_DISK_CSV):
            ts_now = time.strftime("%H%M%S")
            shutil.copy2(RAM_DISK_CSV, os.path.join(BACKUP_DIR, f"topology_log_{SESSION_ID}_{ts_now}.csv"))
            if os.path.exists(PERFORMANCE_CSV):
                shutil.copy2(PERFORMANCE_CSV, os.path.join(BACKUP_DIR, f"performance_log_{SESSION_ID}_{ts_now}.csv"))

# ==========================================
# LUỒNG 3: FASTAPI & WEBSOCKETS
# ==========================================
def serialize_graph(g_nx):
    nodes = []
    for n, d in g_nx.nodes(data=True):
        is_gw = (n == GATEWAY_NODE)
        nodes.append({
            "id": n, "label": f"Node {n}\n(Grad: {d.get('grad', 0 if is_gw else '?')})", 
            "color": "red" if is_gw else d.get('color', 'blue'),
            "level": 0 if is_gw else (int(d.get('grad', 3)) if str(d.get('grad')).isdigit() else 3)
        })
    edges = []
    for u, v, d in g_nx.edges(data=True):
        is_p = d.get('is_parent')
        edges.append({
            "from": u, "to": v, "label": f"{d.get('rssi')}dBm\nC: {d.get('cost')}", 
            "width": 3 if is_p else 1, "dashes": not is_p, "color": "#ff4d4d" if is_p else "gray"
        })
    return {"nodes": nodes, "edges": edges}

def graph_to_json():
    with graph_lock:
        data = serialize_graph(Master_Graph)
    return json.dumps({"before": data, "after": data}) # Same for pure gradient

@app.get("/")
async def get_dashboard():
    return FileResponse("index.html")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    await websocket.send_text(latest_graph_json if latest_graph_json else graph_to_json())
    async def listen():
        try:
            while True:
                data = await websocket.receive_text()
                cmd = json.loads(data)
                send_uart_command(f"mesh backprop {cmd['target']} {cmd['led']}")
        except: pass
    async def broadcast():
        try:
            while True:
                await websocket.send_text(latest_graph_json)
                await asyncio.sleep(1)
        except: pass
    await asyncio.gather(listen(), broadcast())
    ws_clients.remove(websocket)

if __name__ == "__main__":
    init_serial() 
    threading.Thread(target=uart_reader_thread, daemon=True).start()
    threading.Thread(target=data_processor_thread, daemon=True).start()
    threading.Thread(target=backup_csv_thread, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
