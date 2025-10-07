"""Shared telemetry generator logic for CLI and GUI use."""
from __future__ import annotations

import random
import struct
import time
from dataclasses import dataclass
from typing import Dict, List

# Steering wheel descriptors
STEERING_WHEEL_DESC: Dict[str, str] = {
    "0x08000000": "regen",
    "0x00040100": "left turn",
    "0x00040000": "left turn",
    "0x00080000": "right turn",
    "0x00080200": "right turn",
    "0x00010000": "horn",
    "0x00020300": "hazards",
    "0x00020000": "hazards",
    "0x00050000": "left horn",
    "0x00090200": "right horn",
    "0x00090000": "right horn",
    "0x08010000": "regen horn",
    "0x08050100": "left regen horn",
    "0x08050000": "left regen horn",
    "0x08090000": "right regen horn",
    "0x08090200": "right regen horn",
    "0x00000000": "none",
}

# Flag descriptions mapped to bit positions
ERROR_FLAGS_BITS: Dict[str, int] = {
    "": 9,
    "Hardware over current": 0,
    "Software over current": 1,
    "DC Bus over voltage": 2,
    "Bad motor position hall sequence": 3,
    "Watchdog caused last reset": 4,
    "Config read error": 5,
    "15V Rail UVLO": 6,
    "Desaturation Fault": 7,
    "Motor Over Speed": 8,
}

LIMIT_FLAGS_BITS: Dict[str, int] = {
    "Output Voltage PWM": 0,
    "Motor Current": 1,
    "Velocity": 2,
    "Bus Current": 3,
    "Bus Voltage Upper Limit": 4,
    "Bus Voltage Lower Limit": 5,
    "IPM/Motor Temperature": 6,
    "": 7,
}


@dataclass
class _Cycler:
    value: int = 0

    def next_index(self, items: List[str]) -> int:
        result = self.value % len(items)
        self.value += 1
        return result


class TelemetryGeneratorCore:
    """Generate pseudo telemetry blocks for exercising parsers."""

    def __init__(self, endianness: str = "big") -> None:
        if endianness not in {"big", "little"}:
            raise ValueError("endianness must be 'big' or 'little'")
        self.endianness = endianness
        self._cycles = {
            "steering": _Cycler(),
            "error": _Cycler(),
            "limit": _Cycler(),
        }

    # ------------------------------ helpers ------------------------------
    def float_to_hex(self, value: float) -> str:
        packed = struct.pack("<f", value)
        if self.endianness == "big":
            packed = packed[::-1]
        return "0x" + packed.hex().upper()

    def int_to_hex(self, value: int) -> str:
        value &= 0xFFFFFFFF
        byte_order = "big" if self.endianness == "big" else "little"
        byte_value = value.to_bytes(4, byteorder=byte_order)
        return "0x" + byte_value.hex().upper()

    def next_steering_key(self) -> str:
        keys = list(STEERING_WHEEL_DESC.keys())
        index = self._cycles["steering"].next_index(keys)
        return keys[index]

    def cycle_flag(self, flag_map: Dict[str, int], cycle_key: str) -> int:
        keys = list(flag_map.keys())
        index = self._cycles[cycle_key].next_index(keys)
        return 1 << flag_map[keys[index]]

    def generate_motor_controller_data(self) -> tuple[str, str]:
        can_receive_error_count = random.randint(0, 5)
        can_transmit_error_count = random.randint(0, 5)
        active_motor_info = random.randint(0, 100)

        error_bits = self.cycle_flag(ERROR_FLAGS_BITS, "error")
        limit_bits = self.cycle_flag(LIMIT_FLAGS_BITS, "limit")
        combined_flags = (error_bits << 16) | limit_bits
        combined_counts_info = (
            (can_receive_error_count << 24)
            | (can_transmit_error_count << 16)
            | active_motor_info
        )
        return self.int_to_hex(combined_counts_info), self.int_to_hex(combined_flags)

    # --------------------------- block builder ---------------------------
    def build_block(self, runtime: str) -> str:
        mc1lim_hex1, mc1lim_hex2 = self.generate_motor_controller_data()
        mc2lim_hex1, mc2lim_hex2 = self.generate_motor_controller_data()

        def f(a: float, b: float) -> str:
            return self.float_to_hex(random.uniform(a, b))

        mc1bus_hex1 = f(0, 160)
        mc1bus_hex2 = f(-20, 90)
        mc1vel_hex1 = self.float_to_hex(float(random.randint(0, 4000)))
        mc1vel_hex2 = f(0, 100)
        mc1tp1_hex1 = f(-40, 180)
        mc1tp1_hex2 = f(-40, 180)
        mc1tp2_hex1 = f(-40, 180)
        mc1tp2_hex2 = f(-40, 180)
        mc1pha_hex1 = f(0, 100)
        mc1pha_hex2 = f(0, 100)
        mc1cum_hex1 = f(0, 40.8)
        mc1cum_hex2 = f(0, 10000)
        mc1vvc_hex1 = f(0, 160)
        mc1vvc_hex2 = f(0, 160)
        mc1ivc_hex1 = f(0, 100)
        mc1ivc_hex2 = f(0, 100)
        mc1bem_hex1 = f(0, 160)
        mc1bem_hex2 = f(0, 160)

        mc2bus_hex1 = f(0, 160)
        mc2bus_hex2 = f(-20, 90)
        mc2vel_hex1 = self.float_to_hex(float(random.randint(0, 4000)))
        mc2vel_hex2 = f(0, 100)
        mc2tp1_hex1 = f(-40, 180)
        mc2tp1_hex2 = f(-40, 180)
        mc2tp2_hex1 = f(-40, 180)
        mc2tp2_hex2 = f(-40, 180)
        mc2vvc_hex1 = f(0, 160)
        mc2vvc_hex2 = f(0, 160)
        mc2pha_hex1 = f(0, 100)
        mc2pha_hex2 = f(0, 100)
        mc2ivc_hex1 = f(0, 100)
        mc2ivc_hex2 = f(0, 100)
        mc2bem_hex1 = f(0, 160)
        mc2bem_hex2 = f(0, 160)
        mc2cum_hex1 = f(0, 40.8)
        mc2cum_hex2 = f(0, 10000)

        dc_drv_hex1 = f(-20000, 20000)
        dc_drv_hex2 = f(0, 100)

        steering_key = self.next_steering_key()
        steering_hex = self.float_to_hex(
            struct.unpack(
                "<f", random.getrandbits(32).to_bytes(4, byteorder="little")
            )[0]
        )

        bp_vmx_hex1 = self.float_to_hex(float(random.randint(0, 50)))
        bp_vmx_hex2 = f(0, 5)
        bp_vmn_hex1 = self.float_to_hex(float(random.randint(0, 50)))
        bp_vmn_hex2 = f(0, 5)
        bp_tmx_hex1 = self.float_to_hex(float(random.randint(0, 50)))
        bp_tmx_hex2 = f(-40, 180)
        bp_ish_hex1 = f(0, 100)
        bp_ish_hex2 = f(-20, 90)
        bp_pvs_hex1 = f(0, 160)
        bp_pvs_hex2 = f(0, 146_880_000)

        return f"""ABCDEF
MC1BUS,{mc1bus_hex1},{mc1bus_hex2}
MC1VEL,{mc1vel_hex1},{mc1vel_hex2}
MC1TP1,{mc1tp1_hex1},{mc1tp1_hex2}
MC1TP2,{mc1tp2_hex1},{mc1tp2_hex2}
MC1PHA,{mc1pha_hex1},{mc1pha_hex2}
MC1CUM,{mc1cum_hex1},{mc1cum_hex2}
MC1VVC,{mc1vvc_hex1},{mc1vvc_hex2}
MC1IVC,{mc1ivc_hex1},{mc1ivc_hex2}
MC1BEM,{mc1bem_hex1},{mc1bem_hex2}
MC2BUS,{mc2bus_hex1},{mc2bus_hex2}
MC2VEL,{mc2vel_hex1},{mc2vel_hex2}
MC2TP1,{mc2tp1_hex1},{mc2tp1_hex2}
MC2TP2,{mc2tp2_hex1},{mc2tp2_hex2}
MC2PHA,{mc2pha_hex1},{mc2pha_hex2}
MC2CUM,{mc2cum_hex1},{mc2cum_hex2}
MC2VVC,{mc2vvc_hex1},{mc2vvc_hex2}
MC2IVC,{mc2ivc_hex1},{mc2ivc_hex2}
MC2BEM,{mc2bem_hex1},{mc2bem_hex2}
DC_DRV,{dc_drv_hex1},{dc_drv_hex2}
DC_SWC,{steering_key},{steering_hex}
BP_VMX,{bp_vmx_hex1},{bp_vmx_hex2}
BP_VMN,{bp_vmn_hex1},{bp_vmn_hex2}
BP_TMX,{bp_tmx_hex1},{bp_tmx_hex2}
BP_ISH,{bp_ish_hex1},{bp_ish_hex2}
BP_PVS,{bp_pvs_hex1},{bp_pvs_hex2}
MC1LIM,{mc1lim_hex1},{mc1lim_hex2}
MC2LIM,{mc2lim_hex1},{mc2lim_hex2}
TL_TIM,{runtime}
UVWXYZ
"""


def calculate_runtime(start_time: float) -> str:
    elapsed_time = int(time.time() - start_time)
    return time.strftime("%H:%M:%S", time.gmtime(elapsed_time))

