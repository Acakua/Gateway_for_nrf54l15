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
import lightgbm as lgb
import numpy as np
import datetime

# ==========================================
# CẤU HÌNH HỆ THỐNG & ĐỊNH DANH PHIÊN (SESSION)
# ==========================================
BAUD_RATE = 115200
POLLING_INTERVAL = 60 
COLLECTION_WINDOW = 30 
PAGE_TIMEOUT = 4 
BACKUP_INTERVAL = 300 
GATEWAY_NODE = "0002" 

SESSION_ID = time.strftime("%Y%m%d_%H%M%S")

if os.name == 'nt':  
    SERIAL_PORT = r'\\.\COM32' 
    RAM_DISK_CSV = f'topology_log_{SESSION_ID}.csv' 
    BACKUP_DIR = 'wsn_backup\\' 
else:                
    SERIAL_PORT = '/dev/serial0'
    RAM_DISK_CSV = f'/dev/shm/topology_log_{SESSION_ID}.csv' 
    BACKUP_DIR = '/home/pi/wsn_backup/'

# [UPD]: Regex bắt 11 nhóm (Drop_Count giờ là uint16, max 65535)
# Format: $[TOPO],Origin,Seq,TotalPg,CurPg,Count,Grad,Parent,Drp(u16),FwdR(u16),Uptime(u32),TotalSent(u32),[neighbors]
TOPO_PATTERN = r"\$\[TOPO\],([0-9A-F]{4}),(\d+),(\d+),(\d+),(\d+),(\d+),([0-9A-F]{4}),(\d+),(\d+),(\d+),(\d+),(.*)"
NEIGHBOR_PATTERN = r"\[([0-9A-F]{4}),(-?\d+),(\d+),(\d+)\]"

# [NEW] Stress Test Log Configuration
STRESS_LOG_HEADERS = [
    "Timestamp", "Type", "SourceAddr", "SenderAddr", "Seq_or_TxCount",
    "HopCount", "Latency_ms", "PathMinRSSI", "BeaconTx", "HeartbeatTx",
    "RouteChanges", "FwdCount", "Rx_Unique_Count", "Remote_Rx_Count", "PDR_Percent"
]
STRESS_LOG_DIR = BACKUP_DIR # Store stress logs in the backup directory

# ==========================================
# BIẾN TOÀN CỤC & KHÓA AN TOÀN
# ==========================================
uart_queue = queue.Queue() 
ws_clients = set() 
Master_Graph = nx.DiGraph()      # Before Dijkstra (Raw Topology)
SDN_Graph = nx.DiGraph()         # After Dijkstra (Optimized Topology)
AI_Model = None                  # LightGBM Model Instance
latest_graph_json = ""
missed_count_dict = {}
current_cycle_data = {}          # Fixed global warning
page_assembly = {}               # Fixed global warning
pending_commit = False           # Flag for Phase 2 piggyback

# [NEW] Stress Test Buffers & Queues
stress_queue = queue.Queue()
stress_rows_buffer = []
latency_sync_cache = {} # { (SourceAddr, Seq): row_index }
rx_stats = {}           # { SourceAddr: set(Seq) }
reported_nodes = set()  # { (SourceAddr, TxCount) }
test_stop_time = None   # [NEW] Timer for delayed export

graph_lock = threading.Lock()
serial_lock = threading.Lock() 

# ------------------------------------------
# LOAD AI MODEL
# ------------------------------------------
# Lấy đường dẫn tuyệt đối tới thư mục chứa file script
BASE_DIR = os.path.dirname(__file__)

# Danh sách các đường dẫn khả thi để tìm model
POSSIBLE_PATHS = [
    os.path.join(BASE_DIR, 'routing_cost_model.txt'),           # Hiện tại trên Pi của bạn
    os.path.join(BASE_DIR, 'ml_training', 'routing_cost_model.txt') # Cấu trúc chuẩn trong repo
]

AI_Model = None
MODEL_LOADED_PATH = None

for path in POSSIBLE_PATHS:
    if os.path.exists(path):
        try:
            AI_Model = lgb.Booster(model_file=path)
            MODEL_LOADED_PATH = path
            print(f"[Hệ Thống] Đã tải model AI LightGBM thành công từ: {path}")
            break
        except Exception as e:
            print(f"[CẢNH BÁO] Lỗi khi load model tại {path}: {e}")

if AI_Model is None:
    print(f"[CẢNH BÁO] Không tìm thấy file model tại các vị trí: {POSSIBLE_PATHS}")
    print("[Hệ Thống] Sẽ dùng heuristic fallback để tính toán routing cost.")

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
# TÍNH TOÁN ROUTING COST PER-LINK (MỖI HÀNG CSV)
# ==========================================
import math

def compute_routing_cost_per_link(grad, nb_rssi, nb_link_up_s, drop_rate, pin, drop_count, fwd_count, neighbor_count, use_ai=True):
    """
    Tính Routing_Cost cho TỪNG LINK riêng biệt (per-neighbor), đảm bảo mỗi
    hàng CSV có giá trị khác nhau dựa vào RSSI và Link_UP của neighbor đó.

    Dùng Model LightGBM (10 features) nếu có, nếu không thì fallback về Heuristic.
    """
    if use_ai and AI_Model is not None:
        try:
            # Features derived to match training:
            # ['Grad', 'RSSI', 'Link_UP(s)', 'Drop_Count', 'Fwd_Count', 
            #  'Drop_Rate(%)', 'Pin(%)', 'Neighbor_Count', 'Link_Stability', 'Load_Per_Neighbor']
            link_stability = math.log1p(nb_link_up_s)
            load_per_neighbor = fwd_count / (neighbor_count + 1)

            features = np.array([[
                grad, nb_rssi, nb_link_up_s, drop_count, fwd_count,
                drop_rate, pin, neighbor_count, link_stability, load_per_neighbor
            ]])
            pred_cost = AI_Model.predict(features)[0]
            return round(pred_cost, 2)
        except Exception as e:
            pass # Fallback to heuristic

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
                            # print(f"[RAW UART] {line}")
                            if "$[TOPO]" in line:
                                uart_queue.put(line)
                            elif "CSV_LOG" in line:
                                stress_queue.put(line)
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
# LUỒNG 2: XỬ LÝ LOGIC, ĐỊNH TUYẾN & SAO LƯU
# ==========================================
def execute_hybrid_push(delta_nodes):
    global pending_commit
    K = len(delta_nodes)
    print(f"\n[AI SDN] Phát hiện {K} node cần thay đổi Next-Hop.")
    
    if K == 0:
        return
        
    if K <= 13:
        print("[AI SDN] K <= 13 -> Dùng chiến lược UNICAST đẩy lệnh.")
        for node, new_parent in delta_nodes:
            payload = 0x8000 | int(new_parent, 16)
            cmd = f"mesh backprop {node} {payload}"
            send_uart_command(cmd)
            time.sleep(0.5) 
    else:
        print(f"[AI SDN] K = {K} > 13 -> Dùng chiến lược BROADCAST gộp lệnh.")
        hex_payload = ""
        for node, new_parent in delta_nodes:
            hex_payload += f"{node}{new_parent}"
        cmd = f"mesh backprop_broadcast {hex_payload}"
        send_uart_command(cmd)
        
    pending_commit = True

def data_processor_thread():
    global current_cycle_data, page_assembly, pending_commit
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
            if pending_commit:
                print("[SDN 2PC] Kích đúp cờ COMMIT vào bản tin topo_req (Phase 2)")
                send_uart_command("mesh topo_req 1")
                pending_commit = False
            else:
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
                # [SỬA]: Lấy đúng 11 tham số từ Regex
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
        except queue.Empty: pass

def commit_topology_to_temp(origin):
    if origin not in page_assembly: return
    data = page_assembly.pop(origin) 
    all_neighbors = []
    for page_num in sorted(data['pages'].keys()):
        all_neighbors.extend(data['pages'][page_num])
        
    pin_est = estimate_battery(data['uptime'], data['total_sent'])
    
    # [SỬA]: Feature Engineering - Tính tỷ lệ Drop Rate (%) cho ML
    total_processed = data['drp'] + data['fwdr']
    drop_rate = 0.0
    if total_processed > 0:
        drop_rate = round((data['drp'] / total_processed) * 100.0, 1)
    
    current_cycle_data[origin] = {
        "grad": data['grad'], 
        "parent": data['parent'], 
        "neighbors": all_neighbors, 
        "is_partial": len(data['pages']) < data['total'],
        "drp": data['drp'],
        "fwdr": data['fwdr'],
        "drop_rate": drop_rate,
        "pin": pin_est
    }

def reconcile_master_graph():
    global Master_Graph, SDN_Graph, latest_graph_json, missed_count_dict
    with graph_lock:
        for node, data in current_cycle_data.items():
            missed_count_dict[node] = 0
            
            Master_Graph.add_node(node, grad=data["grad"], color="green", drp=data["drp"], fwdr=data["fwdr"], drop_rate=data["drop_rate"], pin=data["pin"])
            Master_Graph.remove_edges_from([(u, v) for u, v in Master_Graph.edges if u == node])
            nb_count = len(data["neighbors"])
            for nb in data["neighbors"]:
                edge_cost = compute_routing_cost_per_link(
                    grad=data["grad"], nb_rssi=nb["rssi"], nb_link_up_s=nb["link_uptime"], 
                    drop_rate=data["drop_rate"], pin=data["pin"],
                    drop_count=data["drp"], fwd_count=data["fwdr"], neighbor_count=nb_count,
                    use_ai=False # Raw View uses Heuristic Formula
                )
                Master_Graph.add_edge(node, nb["addr"], rssi=nb["rssi"], is_parent=(nb["addr"] == data["parent"]), 
                                      link_uptime=nb["link_uptime"], cost=edge_cost)
            
            # [FIX] Force-add Reported Parent edge if it's missing (e.g. not in neighbor list due to overflow)
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
                    
        # --- SDN OPTIMIZATION (DIJKSTRA) ---
        SDN_Graph = Master_Graph.copy()
        for node in list(SDN_Graph.nodes):
            if node == GATEWAY_NODE: continue
            
            # [NEW] Recalculate costs for SDN Graph using AI predictions
            if node in current_cycle_data:
                data = current_cycle_data[node]
                nb_count = len(data["neighbors"])
                for u, v, d in list(SDN_Graph.edges(node, data=True)):
                    # Find neighbor data to get specific link_uptime/rssi
                    nb_data = next((n for n in data["neighbors"] if n["addr"] == v), None)
                    if nb_data:
                        ai_cost = compute_routing_cost_per_link(
                            grad=data["grad"], nb_rssi=nb_data["rssi"], nb_link_up_s=nb_data["link_uptime"],
                            drop_rate=data["drop_rate"], pin=data["pin"],
                            drop_count=data["drp"], fwd_count=data["fwdr"], neighbor_count=nb_count,
                            use_ai=True # SDN View uses AI Prediction
                        )
                        SDN_Graph[u][v]['cost'] = ai_cost

            try:
                # Find shortest path from node to Gateway using AI cost
                path = nx.shortest_path(SDN_Graph, source=node, target=GATEWAY_NODE, weight='cost')
                best_parent = path[1] if len(path) > 1 else None
                
                # Update Edge styles in SDN Graph to reflect the new parent
                for u, v, d in SDN_Graph.edges(data=True):
                    if u == node:
                        SDN_Graph[u][v]['is_parent'] = (v == best_parent)
            except nx.NetworkXNoPath:
                pass # Unreachable
                
        # --- DELTA FILTERING AND HYBRID PUSH ---
        delta_nodes = []
        for node in SDN_Graph.nodes:
            if node == GATEWAY_NODE: continue
            
            ai_parent = None
            for u, v, d in SDN_Graph.edges(data=True):
                if u == node and d.get('is_parent', False):
                    ai_parent = v
                    break
            
            # [NEW] Check if the ACTUAL parent reported by node matches AI recommendation
            actual_parent = None
            if node in Master_Graph:
                for u, v, d in Master_Graph.edges(data=True):
                    if u == node and d.get('is_parent', False):
                        actual_parent = v
                        # If matches AI, mark for styling
                        Master_Graph[u][v]['is_ai_optimized'] = (v == ai_parent)
                        break
                    
            if ai_parent and actual_parent and ai_parent != actual_parent:
                delta_nodes.append((node, ai_parent))
                
        if len(delta_nodes) > 0:
            threading.Thread(target=execute_hybrid_push, args=(delta_nodes,), daemon=True).start()
                
    latest_graph_json = graph_to_json()

def save_to_csv_ramdisk():
    try:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        file_exists = os.path.isfile(RAM_DISK_CSV)
        with open(RAM_DISK_CSV, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "Timestamp", "Origin", "Grad", "Parent",
                    "Neighbor", "RSSI", "Link_UP(s)",
                    "Drop_Count", "Fwd_Count", "Drop_Rate(%)", "Pin(%)",
                    "Routing_Cost", "AI_Routing_Cost"
                ])
            for origin, data in current_cycle_data.items():
                safe_origin = f'="{origin}"'
                safe_parent = f'="{data["parent"]}"'
                nb_count = len(data["neighbors"])
                for nb in data["neighbors"]:
                    safe_neighbor = f'="{nb["addr"]}"'
                    heuristic_cost = compute_routing_cost_per_link(
                        grad         = data["grad"],
                        nb_rssi      = nb["rssi"],
                        nb_link_up_s = nb["link_uptime"],
                        drop_rate    = data["drop_rate"],
                        pin          = data["pin"],
                        drop_count   = data["drp"],
                        fwd_count    = data["fwdr"],
                        neighbor_count = nb_count,
                        use_ai       = False
                    )
                    ai_cost = compute_routing_cost_per_link(
                        grad         = data["grad"],
                        nb_rssi      = nb["rssi"],
                        nb_link_up_s = nb["link_uptime"],
                        drop_rate    = data["drop_rate"],
                        pin          = data["pin"],
                        drop_count   = data["drp"],
                        fwd_count    = data["fwdr"],
                        neighbor_count = nb_count,
                        use_ai       = True
                    )
                    writer.writerow([
                        ts, safe_origin, data["grad"], safe_parent,
                        safe_neighbor, nb["rssi"], nb["link_uptime"],
                        data["drp"], data["fwdr"], data["drop_rate"],
                        data["pin"], heuristic_cost, ai_cost
                    ])
    except Exception as e:
        pass

def backup_csv_thread():
    if not os.path.exists(BACKUP_DIR): os.makedirs(BACKUP_DIR)
    while True:
        time.sleep(BACKUP_INTERVAL)
        if os.path.exists(RAM_DISK_CSV):
            ts_now = time.strftime("%H%M%S")
            backup_filename = f"topology_snapshot_{SESSION_ID}_{ts_now}.csv"
            try:
                shutil.copy2(RAM_DISK_CSV, os.path.join(BACKUP_DIR, backup_filename))
            except Exception as e:
                pass

# ==========================================
# LUỒNG 4: XỬ LÝ STRESS TEST LOG (MIMIC SINK_LOGGER)
# ==========================================
def safe_int_convert(val):
    try:
        val = str(val).strip()
        if val.lower().startswith('0x'):
            return int(val, 16)
        return int(val)
    except (ValueError, TypeError):
        return 0

def export_stress_log():
    """Ghi lại file CSV stress test cuối cùng với mã hóa utf-8-sig."""
    if not stress_rows_buffer:
        return
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    final_filename = os.path.join(STRESS_LOG_DIR, f"FINAL_STRESS_{SESSION_ID}_{timestamp}.csv")
    
    try:
        if not os.path.exists(STRESS_LOG_DIR):
            os.makedirs(STRESS_LOG_DIR)
            
        with open(final_filename, mode='w', newline='', encoding='utf-8-sig') as f:
            w = csv.writer(f)
            w.writerow(STRESS_LOG_HEADERS)
            w.writerows(stress_rows_buffer)
        print(f"\n[Hệ Thống] Đã xuất báo cáo Stress Test: {final_filename}")
    except Exception as e:
        print(f"[LỖI] Không thể xuất file CSV stress: {e}")

def stress_processor_thread():
    global stress_rows_buffer, latency_sync_cache, rx_stats, reported_nodes, test_stop_time
    print("[Luồng 4] Khởi động Xử lý Stress Test Log")
    
    while True:
        try:
            # [NEW] Check if 10 minutes passed since TEST_STOP
            if test_stop_time and (time.time() - test_stop_time > 600):
                print("\n[STRESS] --- Hết thời gian chờ 10 phút. Đang xuất file... ---")
                export_stress_log()
                test_stop_time = None

            line = stress_queue.get(timeout=0.1)
            if "CSV_LOG," not in line:
                continue
                
            raw_data = line.split("CSV_LOG,")[1]
            parts = [p.strip() for p in raw_data.split(',')]
            if not parts:
                continue
                
            # Correct slicing and conversion
            now = datetime.datetime.now().strftime('%H:%M:%S.%f')
            now = now[:-3] if len(now) > 3 else now
            log_type = parts[0]
            
            row = [""] * len(STRESS_LOG_HEADERS)
            row[0] = now
            row[1] = log_type
            
            if log_type == "DATA" and len(parts) >= 6:
                src_val = safe_int_convert(parts[1])
                sender_val = safe_int_convert(parts[2])
                seq_val = safe_int_convert(parts[3])
                
                src_hex = f"0x{src_val:04x}"
                sender_hex = f"0x{sender_val:04x}"
                
                row[2] = src_hex
                row[3] = sender_hex
                row[4] = str(seq_val)
                row[5] = parts[4] # Hops
                row[6] = "0"      # Latency (Chờ PONG report)
                row[7] = parts[7] if len(parts) > 7 else (parts[6] if len(parts) > 6 else "-99")
                
                latency_sync_cache[(src_hex, seq_val)] = len(stress_rows_buffer)
                stress_rows_buffer.append(row)
                
                if src_hex not in rx_stats:
                    rx_stats[src_hex] = set()
                rx_stats[src_hex].add(seq_val)

            elif log_type == "RTT_DATA" and len(parts) >= 4:
                src_hex = f"0x{safe_int_convert(parts[1]):04x}"
                seq_val = safe_int_convert(parts[2])
                rtt_val = parts[3]
                
                key = (src_hex, seq_val)
                if key in latency_sync_cache:
                    row_idx = latency_sync_cache[key]
                    # Ensure we are modifying the list element
                    if row_idx < len(stress_rows_buffer):
                        stress_rows_buffer[row_idx][6] = rtt_val
                continue # RTT_DATA chỉ để cập nhật Latency, không ghi dòng riêng

            elif log_type == "HEARTBEAT" and len(parts) >= 5:
                src_hex = f"0x{safe_int_convert(parts[1]):04x}"
                row[2] = src_hex
                row[3] = f"0x{safe_int_convert(parts[2]):04x}"
                row[5] = parts[3] # Hops
                row[6] = parts[4] # Latency
                stress_rows_buffer.append(row)

            elif log_type == "REPORT" and len(parts) >= 7:
                src_hex = f"0x{safe_int_convert(parts[1]):04x}"
                tx_count = parts[2]
                report_key = (src_hex, tx_count)
                
                if report_key in reported_nodes:
                    continue
                
                reported_nodes.add(report_key)
                data_tx = safe_int_convert(tx_count)
                measured_rx = len(rx_stats.get(src_hex, set()))
                pdr = (measured_rx / data_tx * 100.0) if data_tx > 0 else 0.0
                remote_rx = parts[7] if len(parts) > 7 else (parts[6] if len(parts) > 6 else "0")

                row[2] = src_hex
                row[4] = str(data_tx)
                row[8] = parts[3] if len(parts) > 3 else "0"  # BeaconTx
                row[9] = parts[4] if len(parts) > 4 else "0"  # HeartbeatTx
                row[10] = parts[5] if len(parts) > 5 else "0" # RouteChanges
                row[11] = parts[6] if len(parts) > 6 else "0" # FwdCount
                row[12] = str(measured_rx) # Unique RX
                row[13] = str(remote_rx)   # Remote RX (if any)
                row[14] = f"{pdr:.2f}"
                stress_rows_buffer.append(row)
                print(f"[STRESS] Node {src_hex} Report: PDR {pdr:.2f}%")

            elif "EVENT" in log_type and len(parts) >= 2:
                event_name = parts[1]
                msg = parts[2] if len(parts) >= 3 else ""
                row[2] = event_name
                row[3] = msg
                
                if "TEST_START" in event_name:
                    test_stop_time = None # Cancel pending export
                    rx_stats.clear()
                    reported_nodes.clear()
                    latency_sync_cache.clear()
                    stress_rows_buffer.clear()
                    stress_rows_buffer.append(row)
                    print("\n[STRESS] --- PHIÊN TEST MỚI BẮT ĐẦU ---")
                
                elif "TEST_STOP" in event_name:
                    stress_rows_buffer.append(row)
                    test_stop_time = time.time()
                    print("\n[STRESS] --- KẾT THÚC TEST. Đang chờ 10 phút để nhận nốt log... ---")
            
        except queue.Empty:
            pass
        except Exception as e:
            print(f"[LUỒNG 4 LỖI] {e}")

# ==========================================
# LUỒNG 5: FASTAPI & WEBSOCKETS CÓ WEB SERVER
# ==========================================
def serialize_graph(g_nx):
    nodes = []
    for n, d in g_nx.nodes(data=True):
        is_gw = (n == GATEWAY_NODE)
        if is_gw:
            tooltip_html = "<b>GATEWAY</b><br>Gradient: 0"
        else:
            tooltip_html = f"<b>Node: {n}</b><br>Gradient: {d.get('grad', '?')}<br>Mất gói (Drop): {d.get('drop_rate', 0.0)}%<br>Thông lượng: {d.get('fwdr', 0)} pkt/30s<br>Dự đoán Pin: {d.get('pin', '?')}%"

        nodes.append({
            "id": n, 
            "label": f"Node {n}\n(Grad: {d.get('grad', 0 if is_gw else '?')})", 
            "color": "red" if is_gw else d.get('color', 'blue'),
            "level": 0 if is_gw else (int(d.get('grad', 3)) if str(d.get('grad')).isdigit() else 3),
            "title": tooltip_html 
        })
    
    edges = []
    for u, v, d in g_nx.edges(data=True):
        is_p = d.get('is_parent')
        is_ai = d.get('is_ai_optimized', False)
        is_v = d.get('is_virtual', False)
        cost = d.get('cost', '?')
        
        rssi_val = d.get('rssi')
        label_text = f"{rssi_val}dBm\nC: {cost}" if not is_v else f"VIRTUAL\nC: {cost}"
        
        # Color Logic:
        # - AI Optimized & Parent: SPRING GREEN (#00FF7F)
        # - Regular Parent: RED
        # - Non-parent: GRAY
        edge_color = "#00FF7F" if is_ai else ("#ff4d4d" if is_p else "gray")
        
        edges.append({
            "from": u, "to": v, "label": label_text, 
            "width": 3 if is_p else 1, "dashes": not is_p, 
            "color": edge_color, "physics": is_p
        })
    return {"nodes": nodes, "edges": edges}

def graph_to_json():
    with graph_lock:
        before_data = serialize_graph(Master_Graph)
        after_data = serialize_graph(SDN_Graph)
    return json.dumps({"before": before_data, "after": after_data})

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
    threading.Thread(target=stress_processor_thread, daemon=True).start()
    threading.Thread(target=backup_csv_thread, daemon=True).start()
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
    except KeyboardInterrupt:
        print("\n[Hệ Thống] Đang dừng Gateway...")
    finally:
        if stress_rows_buffer:
            print("[Hệ Thống] Đang thực hiện lưu log stress test cuối cùng...")
            export_stress_log()