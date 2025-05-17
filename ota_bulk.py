# ota_bulk.py
import hashlib, struct, pathlib, time, re, sys, threading
from datetime import datetime
import paho.mqtt.client as mqtt

BROKER = "147.93.127.215"          # change if broker is elsewhere
PORT   = 1883
CHUNK  = 120                  # must match ESP32's sketch
QOS    = 1

_fb_re = re.compile(r"ota/feedback/(.+)")

class BulkOTA:
    """Publish <fw_path> to every chip in `device_ids`"""
    def __init__(self, fw_path: str, device_ids: list[str]):
        self.fw    = pathlib.Path(fw_path).read_bytes()
        self.n     = (len(self.fw)+CHUNK-1)//CHUNK
        self.md5 = hashlib.md5(self.fw).hexdigest()
        self.chips = {cid: {"idx": 0, "done": False} for cid in device_ids}
        # ota_bulk.py  –  around line 87-90

        self.cli   = mqtt.Client(
            client_id="bulk-ota-uploader-"+datetime.utcnow().isoformat(),
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1   # ← add this
        )

        self.cli.on_message = self._on_msg
        self.cli.on_connect = self._on_connect
        self.cli.connect(BROKER, PORT, 60)
        self.cli.loop_start()

    # ── building chunks ────────────────────────────────────────
    def _chunk(self, i:int):
        payload = self.fw[i*CHUNK:(i+1)*CHUNK]
        size    = len(payload).to_bytes(2,"big")
        md5     = hashlib.md5(payload).digest()
        remain  = (self.n-i-1).to_bytes(2,"big")
        return size + payload + md5 + remain

    # ── MQTT callbacks ─────────────────────────────────────────
    def _on_connect(self, cli, *_):
        cli.subscribe("ota/feedback/#", qos=QOS)
        print("[BulkOTA] connected, waiting for devices…")

    def _on_msg(self, cli, _, msg):
        m = _fb_re.match(msg.topic)
        if not m:
            return
        chip = m.group(1)                  # ← re-indented correctly
        if chip not in self.chips:
            return
        fb = msg.payload.decode()

        if fb == "ready":
            # first send overall-image MD5 so the ESP can call Update.setMD5()
            cli.publish(f"ota/{chip}", f"md5:{self.md5}", qos=QOS)

            # then send the 4-byte size frame (starts the chunk sequence)
            cli.publish(f"ota/{chip}", len(self.fw).to_bytes(4, "big"), qos=QOS)

            # ensure the chip entry exists
            self.chips[chip] = {"idx": 0, "done": False}


        elif fb == "ok":
            st = self.chips[chip]
            if st["idx"] < self.n:
                cli.publish(f"ota/{chip}", self._chunk(st["idx"]), qos=QOS)
                st["idx"] += 1
                print(f"{chip}: {st['idx']}/{self.n}")

        elif fb == "success":
            self.chips[chip]["done"] = True
            print(f"{chip}: ✓ done")

    # ── helper for Flask thread ────────────────────────────────
    def wait(self, timeout_s=None):
        """Block until every device sent 'success' or until timeout_s.
           Pass timeout_s=None for no timeout."""
        start = time.time()
        while True:
            if all(c["done"] for c in self.chips.values()):
                break                       # all boards finished
            if timeout_s and (time.time() - start) > timeout_s:
                print("[BulkOTA] timeout – aborting")
                break
            time.sleep(1)

        self.cli.loop_stop()
        self.cli.disconnect()

