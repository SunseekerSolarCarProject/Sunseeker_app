"""Streaming CAN decoder utilities for Sunseeker data tools."""

from typing import Callable, List, Dict, Any, Optional, Tuple
import pandas as pd
import os
import struct
from concurrent.futures import ThreadPoolExecutor

# ---- Memory-safe streaming + schema helpers ----
EXCEL_XLSX_MAX_ROWS = 1_048_576
EXCEL_XLS_MAX_ROWS  = 65_536

CHUNK_ROWS   = 300_000   # how many input rows to read per chunk
FLUSH_EVERY  = 10_000    # flush decoded rows to disk every N rows (keeps RAM small)

def known_columns(include_bits: bool) -> list:
    base = [
        "timestamp","id_hex","message","channel","raw_data",
        "base_hex","offset","sub_id",
        # --- Prohelion / Car (CAN0) names (no *_counts) ---
        "bus_current_A","bus_voltage_V",
        "vehicle_velocity_mps","motor_velocity_rpm",
        "phase_c_current_arms","phase_b_current_arms",
        "Vd","Vq","Id","Iq","BEMFd","BEMFq",
        "heatsink_temp_C","motor_temp_C","dsp_temp_C",
        "dc_bus_Ah","odometer_m","rail_15V","rail_3v3","rail_1v9",
        # limit/status bitfields we still keep as bits (not counts)
        *[f"mc_limit_bit{i}" for i in range(16)],
        *[f"dc_switch_pos_bit{i}" for i in range(16)],
        *[f"dc_switch_change_bit{i}" for i in range(16)],
        *[f"stw_pos_bit{i}" for i in range(16)],
        *[f"stw_change_bit{i}" for i in range(16)],
        "stw_pos_horn","stw_pos_ind_l","stw_pos_ind_r","stw_pos_regen","stw_pos_cruise",
        # --- Battery Protection (car-bus summary floats) ---
        "max_cell_voltage_V","max_cell_id",
        "min_cell_voltage_V","min_cell_id",
        "max_temp_C","max_temp_cell",
        "shunt_current_A","state_of_charge_pct",
        "pack_voltage_V","shunt_sum",
        "mppt_onoff_cmd",
        # --- Orion CAN1 (unchanged) ---
        "high_cell_voltage_V","high_cell_id","low_cell_voltage_V","low_cell_id",
        "pack_current_A","pack_voltage_V_orion","state_of_charge_pct_orion","num_cells",
        "high_temp_C_orion","high_temp_id","low_temp_C_orion","low_temp_id",
        "flag0_low_cell_voltage","flag0_high_cell_voltage","flag0_over_temp","flag0_open_wiring",
        "flag0_internal_comm","flag0_charge_enable_relay","flag0_discharge_relay","flag0_blank",
        "raw_flags_hex",
        "cell_id","inst_voltage_V","internal_resistance_mOhm","shunting",
        "open_circuit_voltage_V","checksum","checksum_ok",
        "thermistor_local_id","temp_C","thermistor_global_id","local_fault","lowest_temp_C",
        "highest_temp_C","module_highest_id","module_lowest_id",
        "unique_id_hex","bms_target_addr","module_number_shifted","const_5","const_6","const_7",
    ]
    if include_bits:
        for i in range(8):
            base += [f"b{i}", f"b{i}_hex", *[f"b{i}_bit{b}" for b in range(8)]]
    return base

class PartitionedCSVWriter:
    """Writes rows to CSV, rolling files so each stays <= row_limit rows."""
    def __init__(self, out_path: str, row_limit: int, columns: list):
        import os
        self.base, self.ext = os.path.splitext(out_path)
        self.row_limit = row_limit
        self.columns = columns
        self.part = 1
        self.count_in_part = 0
        self.paths = []
        self.header_written = False

    def _path(self) -> str:
        # first file uses the exact name; next parts add _partN
        if self.part == 1:
            return f"{self.base}{self.ext}"
        return f"{self.base}_part{self.part}{self.ext}"

    def _roll(self):
        self.part += 1
        self.count_in_part = 0
        self.header_written = False

    def write_df(self, df):
        import pandas as pd
        if df.empty: 
            return
        # align to fixed schema so headers remain consistent
        df = df.reindex(columns=self.columns)
        start = 0
        n = len(df)
        while start < n:
            room = self.row_limit - self.count_in_part
            if room <= 0:
                self._roll()
            take = min(room, n - start)
            chunk = df.iloc[start:start+take]
            path = self._path()
            if path not in self.paths:
                self.paths.append(path)
            mode = "a" if self.header_written else "w"
            chunk.to_csv(path, index=False, header=not self.header_written, mode=mode)
            self.header_written = True
            self.count_in_part += len(chunk)
            start += take

# --- Adjustable scaling for CAN0 (vehicle) + misc. ---
# Values below are common defaults for WaveSculptor-style logs.
# If you have a spec sheet with exact scalars, tweak here.
SCALING = {
    "MC_BUS":        {"I_A": 0.1,   "V_V": 0.1},
    "MC_VELOCITY":   {"vehicle_mps": 0.01, "motor_rpm": 1.0},
    "MC_PHASE":      {"Ic_A": 0.1,  "Ib_A": 0.1},
    "MC_V_VECTOR":   {"Vd_V": 0.1,  "Vq_V": 0.1},
    "MC_I_VECTOR":   {"Id_A": 0.1,  "Iq_A": 0.1},
    "MC_BEMF_VECTOR":{"BEMFd_V": 0.1, "BEMFq_V": 0.1},
    "MC_TEMP1":      {"heatsink_C": 0.1, "motor_C": 0.1},
    "MC_TEMP2":      {"dsp_C": 0.1},
    "MC_CUMULATIVE": {"dc_bus_Ah": 0.1, "odometer_m": 1.0},
    "MC_RAIL1":      {"rail_15V_V": 0.01},
    "MC_RAIL2":      {"rail_3v3_V": 0.01, "rail_1v9_V": 0.01},

    # Driver Controls (setpoints)
    "DC_DRIVE":      {"motor_current_setpoint_A": 0.1, "motor_velocity_setpoint_rpm": 1.0},
    "DC_POWER":      {"bus_current_setpoint_A": 0.1},

    # Battery Protection (approximate — adjust if you have the spec)
    "BP_VMAX":       {"max_voltage_V": 0.0001},   # mV -> V
    "BP_VMIN":       {"min_voltage_V": 0.0001},   # mV -> V
    "BP_TMAX":       {"max_temp_C": 0.1},        # 0.1 °C/ct
    "BP_ISH":        {"shunt_current_A": 0.1, "battery_soc_pct": 0.5},
    "BP_PVSS":       {"pack_voltage_V": 0.1, "shunt_sum_Ah": 0.1},
}

# ---------- helpers ----------
def u16_le(b0, b1): return (b0 | (b1 << 8)) & 0xFFFF
def s16_le(b0, b1):
    v = u16_le(b0, b1)
    return v - 0x10000 if v & 0x8000 else v
def u16_be(b0, b1): return ((b0 << 8) | b1) & 0xFFFF
def u32_le(b0,b1,b2,b3): return (b0 | (b1<<8) | (b2<<16) | (b3<<24)) & 0xFFFFFFFF
def s8(v): return v - 256 if v >= 128 else v

def compute_checksum(broadcast_id: int, data: bytes) -> int:
    # Orion cell-broadcast checksum = LSB( ID + 8 + sum(bytes[0..6]) )
    return (broadcast_id + 8 + sum(data[:7])) & 0xFF

def expand_bits_word(word: int, prefix: str) -> Dict[str, int]:
    return {f"{prefix}_bit{b}": 1 if (word >> b) & 1 else 0 for b in range(16)}

def expand_bits_bytes(data: bytes, prefix: str="b") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for i, b in enumerate(data):
        out[f"{prefix}{i}"] = b
        out[f"{prefix}{i}_hex"] = f"{b:02X}"
        for bit in range(8):
            out[f"{prefix}{i}_bit{bit}"] = 1 if ((b >> bit) & 1) else 0
    return out

def floats_le(d: bytes) -> tuple[float, float]:
    # returns (hi32, lo32) to match Prohelion tables: 63..32 first, 31..0 second
    lo, hi = struct.unpack("<ff", d)
    return hi, lo

def parse_can_id(val) -> int:
    s = str(val).strip()
    # treat all IDs as hex for logs like "6B4" or "0x6B4"
    if s.lower().startswith("0x"):
        return int(s, 16)
    return int(s, 16)

# ---------- CAN1 (Orion + Thermistor) ----------
OR_CAN_BASE = 0x6B0
OR_SVOLT = OR_CAN_BASE + 0x00   # 0x6B0 High/Low cell summary
OR_PACK  = OR_CAN_BASE + 0x01   # 0x6B1 Pack current/voltage/SOC/cell count
OR_STEMP = OR_CAN_BASE + 0x02   # 0x6B2 High/Low temps + IDs
OR_FLAGS = OR_CAN_BASE + 0x03   # 0x6B3 Custom flags (byte 0 used here)
OR_AVOLT = OR_CAN_BASE + 0x04   # 0x6B4 Battery Cell Broadcast
OR_ATEMP = OR_CAN_BASE + 0x05   # 0x6B5 Extra temps

THERM_GENERAL   = 0x1838F380   # J1939 thermistor general
THERM_ADDRCLAIM = 0x18EEFF80   # J1939 address claim

def decode_or_svolt(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    hi_v = u16_le(d[0], d[1]) * 1e-4
    hi_id = d[2]
    lo_v = u16_le(d[4], d[5]) * 1e-4
    lo_id = d[6]
    return [dict(timestamp=ts, id_hex=hex(can_id), message="OR_SVOLT",
                 high_cell_voltage_V=round(hi_v,4), high_cell_id=hi_id,
                 low_cell_voltage_V=round(lo_v,4), low_cell_id=lo_id)]

def decode_or_pack(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    pack_current_A = s16_le(d[0], d[1]) / 10.0
    pack_voltage_V = u16_le(d[2], d[3]) / 10.0
    soc_pct        = u16_le(d[4], d[5]) * 0.5
    num_cells      = u16_le(d[6], d[7])
    return [dict(timestamp=ts, id_hex=hex(can_id), message="OR_PACK",
                 pack_current_A=pack_current_A,
                 pack_voltage_V=pack_voltage_V,
                 state_of_charge_pct=soc_pct,
                 num_cells=num_cells)]

def decode_or_stemp(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    hi_temp_C = s16_le(d[0], d[1]); hi_id = u16_le(d[2], d[3])
    lo_temp_C = s16_le(d[4], d[5]); lo_id = u16_le(d[6], d[7])
    return [dict(timestamp=ts, id_hex=hex(can_id), message="OR_STEMP",
                 high_temp_C=hi_temp_C, high_temp_id=hi_id,
                 low_temp_C=lo_temp_C, low_temp_id=lo_id)]

def decode_or_flags(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    bit_names = [
        "Low Cell Voltage","High Cell Voltage","Over Temp","Open Wiring",
        "Internal Comm","Charge Enable Relay","Discharge Relay","Blank"
    ]
    flags0 = d[0]
    fields = {f"flag0_{n.replace(' ','_').lower()}": bool(flags0 & (1<<i))
              for i,n in enumerate(bit_names)}
    fields["raw_flags_hex"] = d.hex()
    return [dict(timestamp=ts, id_hex=hex(can_id), message="OR_FLAGS", **fields)]

def decode_or_avolt(ts, can_id, d: bytes, include_bits=False) -> List[Dict[str,Any]]:
    # Orion O2/Jr cell broadcast format
    cell_id        = d[0]
    inst_voltage_V = u16_be(d[1], d[2]) * 1e-4
    shunting       = bool(d[3] & 0x80)
    res_raw        = ((d[3] & 0x7F) << 8) | d[4]
    internal_mOhm  = res_raw * 0.01
    ocv_V          = u16_be(d[5], d[6]) * 1e-4
    checksum       = d[7]
    checksum_ok    = (checksum == compute_checksum(can_id, d))
    base = dict(timestamp=ts, id_hex=hex(can_id), message="OR_AVOLT",
                cell_id=cell_id,
                inst_voltage_V=round(inst_voltage_V,4),
                internal_resistance_mOhm=round(internal_mOhm,2),
                shunting=shunting,
                open_circuit_voltage_V=round(ocv_V,4),
                checksum=checksum, checksum_ok=checksum_ok)
    if include_bits:
        base.update(expand_bits_bytes(d))
    return [base]

def decode_or_atemp(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    rows = []
    for i in range(0,8,2):
        tid, temp = d[i], s8(d[i+1])
        if tid==0 and temp==0 and i>0:
            continue
        rows.append(dict(timestamp=ts, id_hex=hex(can_id), message="OR_ATEMP",
                         thermistor_local_id=tid, temp_C=temp))
    return rows

def decode_therm_general(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    return [dict(timestamp=ts, id_hex=hex(can_id), message="THERM_GENERAL",
                 thermistor_global_id=d[0], temp_C=s8(d[1]),
                 thermistor_local_id=(d[2] & 0x7F), local_fault=bool(d[2] & 0x80),
                 lowest_temp_C=s8(d[3]), highest_temp_C=s8(d[4]),
                 module_highest_id=d[5], module_lowest_id=d[6], reserved=d[7])]

def decode_therm_addrclaim(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    return [dict(timestamp=ts, id_hex=hex(can_id), message="THERM_ADDR_CLAIM",
                 unique_id_hex=d[0:3].hex(),
                 bms_target_addr=d[3], module_number_shifted=d[4],
                 const_5=d[5], const_6=d[6], const_7=d[7])]

# ---------- CAN2 (MC/DC/STW/BP/MPPT) ----------
MC_CAN_BASE1 = 0x400
MC_CAN_BASE2 = 0x420
DC_CAN_BASE  = 0x500
STW_CAN_BASE = 0x540
BP_CAN_BASE  = 0x580
MPPT_CAN_BASE = 0x600
MPPT_CAN_ONOFF = MPPT_CAN_BASE + 0x10

MC_OFFSETS = {
    0x01: "MC_LIMITS",
    0x02: "MC_BUS",
    0x03: "MC_VELOCITY",
    0x04: "MC_PHASE",
    0x05: "MC_V_VECTOR",
    0x06: "MC_I_VECTOR",
    0x07: "MC_BEMF_VECTOR",
    0x08: "MC_RAIL1",
    0x09: "MC_RAIL2",
    0x0B: "MC_TEMP1",
    0x0C: "MC_TEMP2",
    0x0E: "MC_CUMULATIVE",
    0x17: "MC_SLIPSPEED",
}
DC_OFFSETS = {0x01:"DC_DRIVE", 0x02:"DC_POWER", 0x03:"DC_RESET", 0x04:"DC_SWITCH"}
STW_OFFSETS = {0x01:"STW_SWITCH"}
BP_OFFSETS  = {0x01:"BP_VMAX",0x02:"BP_VMIN",0x03:"BP_TMAX",0x04:"BP_PCDONE",0x05:"BP_ISH",0x06:"BP_PVSS",0x07:"BP_RESET"}
STW_FLAGS   = {0:"HORN",1:"IND_L",2:"IND_R",3:"REGEN",4:"CRUISE"}

def split_words_le(d: bytes):
    return (u16_le(d[0],d[1]), u16_le(d[2],d[3]), u16_le(d[4],d[5]), u16_le(d[6],d[7]))

def decode_mc(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    base = MC_CAN_BASE1 if (MC_CAN_BASE1 <= can_id < MC_CAN_BASE1+0x20) else MC_CAN_BASE2
    offset = can_id - base
    name = MC_OFFSETS.get(offset, f"MC_UNKNOWN_{offset:02X}")
    f_hi, f_lo = floats_le(d)  # 63..32 (table's first var), 31..0 (second var)
    row = dict(timestamp=ts, id_hex=hex(can_id), message=name, base_hex=hex(base), offset=offset)

    if name == "MC_BUS":                 # A, V
        row.update(bus_current_A=f_hi, bus_voltage_V=f_lo)  # :contentReference[oaicite:4]{index=4}
    elif name == "MC_VELOCITY":         # m/s, rpm
        row.update(vehicle_velocity_mps=f_hi, motor_velocity_rpm=f_lo)  # :contentReference[oaicite:5]{index=5}
    elif name == "MC_PHASE":            # A_rms, A_rms
        row.update(phase_c_current_arms=f_hi, phase_b_current_arms=f_lo)
    elif name == "MC_V_VECTOR":         # volts in D/Q frame
        row.update(Vd=f_hi, Vq=f_lo)
    elif name == "MC_I_VECTOR":         # amps in D/Q frame
        row.update(Id=f_hi, Iq=f_lo)
    elif name == "MC_BEMF_VECTOR":      # volts in D/Q frame
        row.update(BEMFd=f_hi, BEMFq=f_lo)
    elif name == "MC_TEMP1":            # °C
        row.update(heatsink_temp_C=f_hi, motor_temp_C=f_lo)
    elif name == "MC_TEMP2":            # °C
        row.update(dsp_temp_C=f_lo)     # keep f_hi if Prohelion documents a second field
    elif name == "MC_CUMULATIVE":       # Ah, m
        row.update(dc_bus_Ah=f_hi, odometer_m=f_lo)
    elif name == "MC_RAIL1":
        row.update(rail_15V=f_hi)
    elif name == "MC_RAIL2":
        row.update(rail_3v3=f_hi, rail_1v9=f_lo)
    elif name == "MC_LIMITS":
        # If you want the limit bits, they live in Status (0x01) per docs; keep any bit expansion you already had
        pass

    return [row]

def decode_dc(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    offset = can_id - DC_CAN_BASE
    name = DC_OFFSETS.get(offset, f"DC_UNKNOWN_{offset:02X}")
    row = dict(timestamp=ts, id_hex=hex(can_id), message=name, offset=offset)
    if name == "DC_DRIVE":
        f_hi, f_lo = floats_le(d)  # % (0..1) and rpm
        row.update(motor_current_pct=f_hi*100.0, motor_velocity_rpm=f_lo)  # % values are 0..1 on CAN. :contentReference[oaicite:6]{index=6}
    elif name == "DC_POWER":
        f_hi, f_lo = floats_le(d)  # bus current command %, second float unused here
        row.update(bus_current_cmd_pct=f_hi*100.0)
    elif name == "DC_SWITCH":
        w0,w1,w2,w3 = split_words_le(d)
        row.update(expand_bits_word(w0, "dc_switch_pos"))
        row.update(expand_bits_word(w1, "dc_switch_change"))
    return [row]

def decode_stw(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    offset = can_id - STW_CAN_BASE
    name = STW_OFFSETS.get(offset, f"STW_UNKNOWN_{offset:02X}")
    w0,w1,w2,w3 = split_words_le(d)
    row = dict(timestamp=ts, id_hex=hex(can_id), message=name)
    if name == "STW_SWITCH":
        row.update(expand_bits_word(w0, "stw_pos"))
        row.update(expand_bits_word(w1, "stw_change"))
        for bit, label in STW_FLAGS.items():
            row[f"stw_pos_{label.lower()}"] = 1 if (w0 >> bit) & 1 else 0
    else:
        row.update(word0=w0, word1=w1, word2=w2, word3=w3)
    return [row]

def decode_bp(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    offset = can_id - BP_CAN_BASE
    name = BP_OFFSETS.get(offset, f"BP_UNKNOWN_{offset:02X}")
    row = dict(timestamp=ts, id_hex=hex(can_id), message=name, offset=offset)
    f_hi, f_lo = floats_le(d)
    if name == "BP_VMAX":
        row.update(max_cell_voltage_V=f_hi, max_cell_id=int(round(f_lo)))
    elif name == "BP_VMIN":
        row.update(min_cell_voltage_V=f_hi, min_cell_id=int(round(f_lo)))
    elif name == "BP_TMAX":
        row.update(max_temp_C=f_hi, max_temp_cell=int(round(f_lo)))
    elif name == "BP_ISH":
        row.update(shunt_current_A=f_hi, state_of_charge_pct=f_lo)   # your firmware sends SOC (0–100) in the other float
    elif name == "BP_PVSS":
        row.update(pack_voltage_V=f_lo, shunt_sum=f_hi)              # pack V in low float, sum/current in high float
    return [row]

def decode_mppt(ts, can_id, d: bytes) -> List[Dict[str,Any]]:
    sub_id = can_id - MPPT_CAN_BASE
    w0,w1,w2,w3 = split_words_le(d)
    row = dict(timestamp=ts, id_hex=hex(can_id), message="MPPT",
               sub_id=sub_id, word0=w0, word1=w1, word2=w2, word3=w3)
    if can_id == MPPT_CAN_ONOFF:
        row["mppt_onoff_cmd"] = d[0]  # presumed 0/1
    return [row]

# Master table for exact IDs (CAN1 + specific MPPT cmd)
DECODERS_EXACT = {
    OR_SVOLT: decode_or_svolt,
    OR_PACK:  decode_or_pack,
    OR_STEMP: decode_or_stemp,
    OR_FLAGS: decode_or_flags,
    OR_AVOLT: lambda ts,cid,d,**kw: decode_or_avolt(ts,cid,d, include_bits=kw.get("include_bits", False)),
    OR_ATEMP: decode_or_atemp,
    THERM_GENERAL: decode_therm_general,
    THERM_ADDRCLAIM: decode_therm_addrclaim,
    MPPT_CAN_ONOFF: decode_mppt,
}

def decode_dynamic(ts, can_id, d: bytes) -> Optional[List[Dict[str,Any]]]:
    if (MC_CAN_BASE1 <= can_id < MC_CAN_BASE1+0x20) or (MC_CAN_BASE2 <= can_id < MC_CAN_BASE2+0x20):
        return decode_mc(ts, can_id, d)
    if (DC_CAN_BASE <= can_id < DC_CAN_BASE+0x10):
        return decode_dc(ts, can_id, d)
    if (STW_CAN_BASE <= can_id < STW_CAN_BASE+0x10):
        return decode_stw(ts, can_id, d)
    if (BP_CAN_BASE <= can_id < BP_CAN_BASE+0x10):
        return decode_bp(ts, can_id, d)
    if (MPPT_CAN_BASE <= can_id < MPPT_CAN_BASE+0x20):
        return decode_mppt(ts, can_id, d)
    return None

# -------- core engine returning a DataFrame (so we can split) --------
def decode_to_dataframe(input_csv: str, include_bits: bool=False, channel: Optional[str]=None) -> pd.DataFrame:
    df = pd.read_csv(input_csv)
    df.columns = [c.strip().lower() for c in df.columns]
    if "id" not in df.columns or "data" not in df.columns:
        raise ValueError("Input CSV must have columns: id, data (and optionally timestamp, channel, dlc).")
    if channel and "channel" in df.columns:
        df = df[df["channel"].astype(str).str.lower() == channel.lower()].copy()
    df = df.dropna(subset=["id"]).copy()
    df["id_int"] = df["id"].apply(parse_can_id)

    out_rows: List[Dict[str,Any]] = []
    for _, r in df.iterrows():
        can_id = int(r["id_int"])
        ts     = r.get("timestamp", "")
        chan   = r.get("channel", None)
        data_s = str(r["data"]).strip()[:16].ljust(16, "0")
        try:
            data_b = bytes.fromhex(data_s)
        except Exception:
            data_b = bytes.fromhex(data_s.replace(" ", ""))

        dec = DECODERS_EXACT.get(can_id)
        recs = dec(ts, can_id, data_b) if dec else decode_dynamic(ts, can_id, data_b)

        if recs is None:
            row = dict(timestamp=ts, id_hex=hex(can_id), message="RAW", raw_data=data_s)
            if include_bits: row.update(expand_bits_bytes(data_b))
            if chan is not None: row["channel"] = chan
            out_rows.append(row)
        else:
            for rr in recs:
                if include_bits: rr.update(expand_bits_bytes(data_b))
                if chan is not None: rr["channel"] = chan
            out_rows.extend(recs)

    return pd.DataFrame(out_rows)

from typing import List, Optional, Dict, Any, Tuple

def _estimate_total_rows(csv_path: str) -> int:
    try:
        with open(csv_path, "rb") as handle:
            total = sum(1 for _ in handle)
    except OSError:
        return 0
    return max(total - 1, 0)


def _stream_decode(
    input_csv: str,
    include_bits: bool,
    channel: Optional[str],
    split: bool,
    out_one: Optional[str],
    out_can1: Optional[str],
    out_can2: Optional[str],
    row_limit: int,
    progress_cb: Optional[Callable[[float, Optional[str]], None]] = None,
) -> List[str]:
    """Memory-safe streaming decode with optional progress callbacks."""
    outputs: List[str] = []
    cols = known_columns(include_bits)
    can1_msgs = {
        "OR_SVOLT",
        "OR_PACK",
        "OR_STEMP",
        "OR_FLAGS",
        "OR_AVOLT",
        "OR_ATEMP",
        "THERM_GENERAL",
        "THERM_ADDRCLAIM",
    }

    total_rows = _estimate_total_rows(input_csv) if progress_cb else 0
    rows_processed = 0

    if progress_cb:
        progress_cb(0.0, "Starting decode")

    writer_one = writer_c0 = writer_c1 = None
    executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(max_workers=2) if split else None

    try:
        if split:
            if out_can1 is None or out_can2 is None:
                raise ValueError("Split output paths must be provided when split=True")
            writer_c0 = PartitionedCSVWriter(out_can1, row_limit, cols)
            writer_c1 = PartitionedCSVWriter(out_can2, row_limit, cols)
        else:
            if out_one is None:
                raise ValueError("Output path must be provided when split=False")
            writer_one = PartitionedCSVWriter(out_one, row_limit, cols)

        need = {"id", "data", "timestamp", "channel"}
        usecols = lambda c: c.strip().lower() in need

        reader = pd.read_csv(
            input_csv,
            chunksize=CHUNK_ROWS,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            on_bad_lines="skip",
            usecols=usecols,
            engine="c",
            low_memory=True,
        )

        for chunk in reader:
            chunk.columns = [c.strip().lower() for c in chunk.columns]

            if channel and "channel" in chunk.columns:
                chunk = chunk[chunk["channel"].str.lower() == channel.lower()]

            if "id" not in chunk.columns or "data" not in chunk.columns:
                rows_processed += len(chunk)
                continue

            buf: List[Dict[str, Any]] = []

            def flush() -> None:
                nonlocal buf
                if not buf:
                    return
                df = pd.DataFrame(buf)
                if split:
                    assert writer_c0 is not None and writer_c1 is not None and executor is not None
                    if "channel" in df.columns:
                        ch = df["channel"].astype(str).str.lower()
                        df_c1 = df[ch == "can1"]
                        df_c0 = df[ch == "can0"]
                    else:
                        is_c1 = df["message"].isin(can1_msgs)
                        df_c1 = df[is_c1]
                        df_c0 = df[~is_c1]
                    futures = []
                    if not df_c0.empty:
                        futures.append(executor.submit(writer_c0.write_df, df_c0))
                    if not df_c1.empty:
                        futures.append(executor.submit(writer_c1.write_df, df_c1))
                    for fut in futures:
                        fut.result()
                else:
                    assert writer_one is not None
                    writer_one.write_df(df)
                buf = []

            for _, r in chunk.iterrows():
                sid = r.get("id", "").strip()
                if not sid:
                    continue
                try:
                    can_id = parse_can_id(sid)
                except Exception:
                    continue

                data_s = str(r.get("data", "")).replace(" ", "").strip()
                data_s = (data_s[:16]).ljust(16, "0")
                try:
                    data_b = bytes.fromhex(data_s)
                except Exception:
                    continue

                ts = r.get("timestamp", "")
                chan = r.get("channel", None)

                dec = DECODERS_EXACT.get(can_id)
                recs = dec(ts, can_id, data_b) if dec else decode_dynamic(ts, can_id, data_b)

                if recs is None:
                    row = dict(timestamp=ts, id_hex=hex(can_id), message="RAW", raw_data=data_s)
                    if include_bits:
                        row.update(expand_bits_bytes(data_b))
                    if chan is not None:
                        row["channel"] = chan
                    buf.append(row)
                else:
                    for rr in recs:
                        if include_bits:
                            rr.update(expand_bits_bytes(data_b))
                        if chan is not None:
                            rr["channel"] = chan
                        buf.append(rr)

                if len(buf) >= FLUSH_EVERY:
                    flush()

            flush()
            rows_processed += len(chunk)
            if progress_cb:
                if total_rows:
                    fraction = min(rows_processed / total_rows, 0.999)
                    progress_cb(fraction, f"Processed {rows_processed} of {total_rows} rows")
                else:
                    progress_cb(0.0, f"Processed {rows_processed} rows")

    finally:
        if executor is not None:
            executor.shutdown(wait=True)

    if progress_cb:
        progress_cb(1.0, "Decoding complete")

    if split:
        assert writer_c0 is not None and writer_c1 is not None
        outputs.extend(writer_c0.paths)
        outputs.extend(writer_c1.paths)
    else:
        assert writer_one is not None
        outputs.extend(writer_one.paths)
    return outputs


def decode_csv_one(
    input_csv: str,
    output_csv: str,
    include_bits: bool=False,
    channel: Optional[str]=None,
    row_limit: int = EXCEL_XLSX_MAX_ROWS,
    progress_cb: Optional[Callable[[float, Optional[str]], None]] = None,
) -> List[str]:
    return _stream_decode(
        input_csv=input_csv,
        include_bits=include_bits,
        channel=channel,
        split=False,
        out_one=output_csv,
        out_can1=None,
        out_can2=None,
        row_limit=row_limit,
        progress_cb=progress_cb,
    )

def decode_csv_split(
    input_csv: str,
    out_can1_csv: str,
    out_can2_csv: str,
    include_bits: bool=False,
    channel: Optional[str]=None,
    row_limit: int = EXCEL_XLSX_MAX_ROWS,
    progress_cb: Optional[Callable[[float, Optional[str]], None]] = None,
) -> List[str]:
    return _stream_decode(
        input_csv=input_csv,
        include_bits=include_bits,
        channel=channel,
        split=True,
        out_one=None,
        out_can1=out_can1_csv,
        out_can2=out_can2_csv,
        row_limit=row_limit,
        progress_cb=progress_cb,
    )
