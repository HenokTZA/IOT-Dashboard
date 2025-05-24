"""from flask import Flask, request, jsonify, render_template
import paho.mqtt.client as mqtt
import os, json

BROKER   = "147.93.127.215"
PORT     = 1883
USER     = os.getenv("MQTT_USER", "iotuser")
PASSWORD = os.getenv("MQTT_PASS", "secretpass")

mqttc = mqtt.Client(callback_api_version=1)
mqttc.username_pw_set(USER, PASSWORD)
mqttc.connect(BROKER, PORT)
mqttc.loop_start()           # background thread

app = Flask(__name__)

@app.route("/")
def index():
    # pass the device id down to the template
    return render_template("index.html", device_id="esp32-123")

@app.route("/api/gpio", methods=["POST"])
def queue_cmd():
    data = request.get_json(force=True)       # ← JSON or 400
    dev, pin, state = data.get("device"), data.get("pin"), data.get("state")
    if None in (dev, pin, state):
        return {"error": "device, pin, state required"}, 400

    topic   = f"devices/{dev}/cmd"
    payload = json.dumps({"pin": int(pin), "state": int(state)})
    mqttc.publish(topic, payload, qos=1)
    return {"queued": True}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)     # debug=auto-reload"""

# app.py
from flask import Flask, request, jsonify, render_template, redirect, url_for, session, flash, send_file
import paho.mqtt.client as mqtt
import os, json, threading, time, uuid
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime          # for precise UTC timestamps
from datetime import datetime, timedelta, UTC   # NEW
from datetime import datetime, timedelta, UTC   # Python 3.11+
from datetime import timezone
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import String, Integer 
from ota_bulk import BulkOTA
from werkzeug.utils import secure_filename
from pathlib import Path
from io import BytesIO
import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from functools import wraps
from flask import abort

db = SQLAlchemy()                       # create the global handle

FIRMWARE_DIR = Path(__file__).parent / "firmware_uploads"
FIRMWARE_DIR.mkdir(exist_ok=True)

BROKER   = "147.93.127.215"
PORT     = 1883
USER     = os.getenv("MQTT_USER", "iotuser")
PASSWORD = os.getenv("MQTT_PASS", "secretpass")

# In-memory user store: { email: {first, last, pw_hash} }
#users = {}

# In‐memory store of devices → status
devices = {}          # e.g. { "esp32-123": "online", ... }
devices_lock = threading.Lock()

telemetry = {}      # { device_id: {volt, curr, temp, ts} }
tele_lock = threading.Lock()

def utc_now():
    return datetime.now(UTC)

def on_mqtt_connect(client, userdata, flags, rc):
    client.subscribe([
        ("devices/+/status", 0),
        ("devices/+/tele",   0),        # NEW
        ("devices/+/ip",     0),
    ])

def on_mqtt_message(client, userdata, msg):
    topic = msg.topic              # e.g. "devices/esp32-123/status" or ".../tele"
    parts = topic.split("/")
    # only handle topics of form devices/<device_id>/<kind>
    if len(parts) != 3 or parts[0] != "devices":
        return

    dev, kind = parts[1], parts[2]


    if kind == "status":
        status = msg.payload.decode()   # "online" or "offline"
        now = utc_now()
        with devices_lock:
            devices[dev] = {"status": status, "ts": now}
            
        touch_device(dev, status)


    elif kind == "tele":
        try:
            data = json.loads(msg.payload)
        except ValueError:
            return  # invalid JSON, ignore

        # store the latest telemetry + a timestamp
        with tele_lock:
            existing = telemetry.get(dev, {})
            existing.update({
                "voltage":     data.get("voltage"),
                "current":     data.get("current"),
                "temperature": data.get("temperature"),
                "ts":          time.time()
            })
            telemetry[dev] = existing

        with app.app_context():
             rec = TelemetryRecord(
                 device_id   = dev,
                 voltage     = data.get("voltage"),
                 current     = data.get("current"),
                 temperature = data.get("temperature"),
             )
             db.session.add(rec)
             db.session.commit() 
        touch_device(dev, "online")


    elif kind == "ip":
        # decode the IP string
        ip_str = msg.payload.decode()
        # store it alongside tele data
        with tele_lock:
            telemetry.setdefault(dev, {})["ip"] = ip_str


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqttc.username_pw_set(USER, PASSWORD)
mqttc.on_connect = on_mqtt_connect
mqttc.on_message = on_mqtt_message
mqttc.connect(BROKER, PORT)
mqttc.loop_start()                       # background thread

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET', 'replace-with-a-secure-random-string')  # for session

# --- DB config ---
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///telemetry.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

class TelemetryRecord(db.Model):
    id          = db.Column(db.Integer, primary_key=True)
    device_id   = db.Column(db.String(64), index=True, nullable=False)
    voltage     = db.Column(db.Float,    nullable=True)
    current     = db.Column(db.Float,    nullable=True)
    temperature = db.Column(db.Float,    nullable=True)
    ts          = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return (f"<Telemetry {self.device_id}: "
                f"{self.voltage} V {self.current} A {self.temperature} °C @ {self.ts}>")


class Device(db.Model):
    id         = db.Column(db.Integer, primary_key=True)
    device_id  = db.Column(db.String(64), unique=True, index=True, nullable=False)
    status     = db.Column(db.String(8), nullable=False)          # "online/offline"
    last_seen  = db.Column(db.DateTime(timezone=True), nullable=False)


# ── model definition (below TelemetryRecord / Device) ─────────────
class User(db.Model):
    id       = db.Column(Integer, primary_key=True)
    email    = db.Column(String(120), unique=True, index=True, nullable=False)
    first    = db.Column(String(50),  nullable=False)
    last     = db.Column(String(50),  nullable=False)
    pw_hash  = db.Column(String(200), nullable=False)
    role     = db.Column(String(10),  default="user")   # NEW  ← "admin" | "user"

    def check_pw(self, raw_pw) -> bool:
        return check_password_hash(self.pw_hash, raw_pw)

class UserDevice(db.Model):
    id         = db.Column(Integer, primary_key=True)
    user_id    = db.Column(Integer, db.ForeignKey(User.id), nullable=False)
    device_id  = db.Column(String(64), nullable=False)      # esp32-…

    db.UniqueConstraint(user_id, device_id)                 # no duplicates

def user_devices(user: User):
    if user.role == "admin":
        # admin sees every row younger than 24 h (your existing logic)
        return Device.query.all()
    else:
        ids = [ud.device_id for ud in UserDevice.query.filter_by(user_id=user.id)]
        return Device.query.filter(Device.device_id.in_(ids)).all()


# Create tables once at startup
with app.app_context():
    db.create_all()

def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = User.query.filter_by(email=session.get('user_email')).first()
        if not user or user.role != 'admin':
            abort(403)
        return fn(*args, **kwargs)
    return wrapper

"""
def purge_stale_devices():
    while True:
        time.sleep(600)                               # run every 10 min
        cutoff = utc_now() - timedelta(hours=24)
        with devices_lock:
            stale = [d for d, info in devices.items()
                     if info["status"] == "offline" and info["ts"] < cutoff]
            for d in stale:
                devices.pop(d, None)
        if stale:
            print(f"[DEV] purged {len(stale)} devices offline >24 h")

threading.Thread(target=purge_stale_devices,
                 daemon=True).start()
"""

def purge_stale_devices():
    while True:
        time.sleep(600)
        cutoff = utc_now() - timedelta(hours=24)

        # delete from DB
        with app.app_context():
            gone = (Device.query
                    .filter(Device.status == "offline",
                            Device.last_seen < cutoff)
                    .delete(synchronize_session=False))
            db.session.commit()

        # mirror in-memory dict
        with devices_lock:
            stale = [d for d, info in devices.items()
                     if info["status"] == "offline" and info["ts"] < cutoff]
            for d in stale:
                devices.pop(d, None)

        if gone or stale:
            print(f"[DEV] purged {gone} DB rows / {len(stale)} cache entries")

"""
def touch_device(dev_id: str, status: str):
    now = utc_now()
    with app.app_context():                       # ensure context in MQTT thread
        d = Device.query.filter_by(device_id=dev_id).first()
        if not d:
            d = Device(device_id=dev_id, status=status, last_seen=now)
            db.session.add(d)
        else:
            d.status    = status
            d.last_seen = now
        db.session.commit()
"""
def _history_df(device_id: str, start_ts: float, end_ts: float):
    rows = (TelemetryRecord.query
            .filter(TelemetryRecord.device_id == device_id,
                    TelemetryRecord.ts >= datetime.fromtimestamp(start_ts, UTC),
                    TelemetryRecord.ts <= datetime.fromtimestamp(end_ts, UTC))
            .order_by(TelemetryRecord.ts.asc())
            .all())
    return pd.DataFrame([{
        "timestamp": r.ts.isoformat(sep=' ', timespec='seconds'),
        "voltage":   r.voltage,
        "current":   r.current,
        "temperature": r.temperature
    } for r in rows])


def touch_device(dev_id: str, status: str):
    now = utc_now()
    with app.app_context():
        d = Device.query.filter_by(device_id=dev_id).first()

        if not d:
            # first time we’ve ever seen this device
            d = Device(device_id=dev_id, status=status, last_seen=now)
            db.session.add(d)

        elif status == d.status == "offline":
            # got a *repeat* offline message → keep original last_seen
            pass

        else:
            # genuine status change or telemetry heartbeat
            d.status    = status
            d.last_seen = now

        db.session.commit()



"""
@app.route("/")
def select_device():
    # Show page listing all online devices
    with devices_lock:
        dev_list = dict(devices)
    return render_template("devices.html", devices=dev_list)
"""
"""
@app.route("/")
def select_device():
    with devices_lock:
        dev_list = {d: info["status"] for d, info in devices.items()}
    return render_template("devices.html", devices=dev_list)
"""
"""
@app.route("/")
def select_device():
    now = utc_now()
    with devices_lock:
        dev_list = {
            d: {
                "status": info["status"],
                "last_seen": int((now - info["ts"]).total_seconds())
            }
            for d, info in devices.items()
        }
    return render_template("devices.html", devices=dev_list)
"""


"""
@app.route("/")
def select_device():
    now = utc_now()                  # aware
    rows = Device.query.all()

    dev_list = {}
    for row in rows:
        # ── handle None or naïve timestamps safely ──
        if row.last_seen is None:
            last_seen_dt = utc_now()             # treat as "just now"
        elif row.last_seen.tzinfo is None:
            last_seen_dt = row.last_seen.replace(tzinfo=timezone.utc)
        else:
            last_seen_dt = row.last_seen

        dev_list[row.device_id] = {
            "status"    : row.status,
            "last_seen" : int((now - last_seen_dt).total_seconds())
        }

    return render_template("devices.html", devices=dev_list)
"""

@app.route("/")
def select_device():
    user = User.query.filter_by(email=session['user_email']).first()
    rows = user_devices(user)

    now = utc_now()
    dev_list = {}
    for row in rows:
        last_seen = row.last_seen if row.last_seen.tzinfo else row.last_seen.replace(tzinfo=timezone.utc)
        dev_list[row.device_id] = {
            "status": row.status,
            "last_seen": int((now - last_seen).total_seconds())
        }
    return render_template("devices.html", devices=dev_list, user=user)



@app.route("/device/<device_id>")
def control_device(device_id):
    # If device isn't known/online, redirect back
    with devices_lock:
        if device_id not in devices:
            return redirect(url_for("select_device"))
    return render_template("device.html", device_id=device_id)

@app.route("/api/telemetry/<device_id>")
def get_telemetry(device_id):
    with tele_lock:
        data = telemetry.get(device_id)
    return (jsonify(data) if data else ("{}", 404))


@app.route("/api/gpio", methods=["POST"])
def queue_cmd():
    data = request.get_json(force=True)
    dev, pin, state = data.get("device"), data.get("pin"), data.get("state")
    if None in (dev, pin, state):
        return {"error": "device, pin, state required"}, 400
    topic   = f"devices/{dev}/cmd"
    payload = json.dumps({"pin": int(pin), "state": int(state)})
    mqttc.publish(topic, payload, qos=1)
    return {"queued": True}, 200


# ───────────────  NEW: historical view  ───────────────
@app.route("/device/<device_id>/history")
def device_history(device_id):
    # latest 5000 samples, newest first – tweak the limit as you wish
    records = (TelemetryRecord.query
               .filter_by(device_id=device_id)
               .order_by(TelemetryRecord.ts.desc())
               .limit(5000)
               .all())
    return render_template("history.html",
                           device_id=device_id,
                           records=records)

"""
@app.route("/device/<device_id>/history")
def device_history(device_id):
    cutoff = datetime.utcnow() - timedelta(hours=8)
    records = (TelemetryRecord.query
               .filter(TelemetryRecord.device_id == device_id,
                       TelemetryRecord.ts >= cutoff)
               .order_by(TelemetryRecord.ts.desc())
               .all())
    return render_template("history.html",
                           device_id=device_id,
                           records=records)
"""

# latest 500 samples (oldest→newest) as raw JSON
@app.route("/api/history/<device_id>")
def get_history_json(device_id):
    rows = (TelemetryRecord.query
            .filter_by(device_id=device_id)
            .order_by(TelemetryRecord.ts.desc())
            .limit(50)
            .all())
    rows.reverse()          # make oldest first
    data = [{
        "ts": r.ts.isoformat(timespec="seconds"),
        "voltage": r.voltage,
        "current": r.current,
        "temperature": r.temperature
    } for r in rows]
    return jsonify(data)

@app.route("/api/export/<fmt>/<device_id>")
def export_history(fmt, device_id):
    # expect ?start=YYYY-MM-DD&end=YYYY-MM-DD
    try:
        start = datetime.fromisoformat(request.args["start"]).replace(tzinfo=UTC)
        end   = datetime.fromisoformat(request.args["end"]).replace(tzinfo=UTC)
    except Exception:
        return {"error": "start & end (YYYY-MM-DD) required"}, 400

    df = _history_df(device_id, start.timestamp(), end.timestamp())
    if df.empty:
        return {"error": "no records"}, 404

    if fmt == "csv":
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(buf,
                         mimetype="text/csv",
                         as_attachment=True,
                         download_name=f"{device_id}_{start.date()}_{end.date()}.csv")

    elif fmt == "pdf":
        # build a ReportLab table PDF in memory
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet

        buf = BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=letter)
        styles = getSampleStyleSheet()
        elems  = []

        # title
        title = Paragraph(f"Historical Data: {device_id} [{start.date()} to {end.date()}]", styles['Heading2'])
        elems.append(title)
        elems.append(Spacer(1, 12))

        # table data
        data = [df.columns.tolist()] + df.values.tolist()
        tbl  = Table(data, repeatRows=1)
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.lightblue),
            ('TEXTCOLOR',   (0,0), (-1,0), colors.white),
            ('GRID',        (0,0), (-1,-1), 0.5, colors.grey),
            ('FONTNAME',    (0,0), (-1,0), 'Helvetica-Bold'),
            ('ALIGN',       (0,0), (-1,-1), 'CENTER'),
            ('VALIGN',      (0,0), (-1,-1), 'MIDDLE'),
        ]))
        elems.append(tbl)

        doc.build(elems)
        buf.seek(0)

        return send_file(buf,
                         mimetype="application/pdf",
                         as_attachment=True,
                         download_name=f"{device_id}_{start.date()}_{end.date()}.pdf")

    else:
        return {"error": "fmt must be csv or pdf"}, 400


@app.route('/add_device', methods=['GET','POST'])
def add_device():
    user = User.query.filter_by(email=session['user_email']).first()
    if user.role == "admin":
        return redirect(url_for('select_device'))

    if request.method == 'POST':
        dev = request.form.get('device_id','').strip().lower()
        exists = Device.query.filter_by(device_id=dev).first()
        if not exists:
            flash('Device not known to system','error')
        else:
            if not UserDevice.query.filter_by(user_id=user.id, device_id=dev).first():
                db.session.add(UserDevice(user_id=user.id, device_id=dev))
                db.session.commit()
            flash('Device added','success')
            return redirect(url_for('select_device'))
    return render_template('add_device.html')


@app.route("/ota")
def ota_form():
    return render_template("ota_upload.html")



@app.route("/api/ota/bulk", methods=["POST"])
def api_ota_bulk():
    """
    POST multipart/form-data with file=<bin>.
    Starts background thread; responds 202 immediately.
    Returns {job_id: …}
    """
    if 'file' not in request.files:
        return {"error": "file field required"}, 400

    fw_file = request.files['file']
    fname   = secure_filename(fw_file.filename or f"fw_{uuid.uuid4().hex}.bin")
    fpath   = FIRMWARE_DIR / fname
    fw_file.save(fpath)

    # list of online devices
    dev_ids = [d.device_id for d in Device.query.filter_by(status="online").all()]
    if not dev_ids:
        return {"error": "no online devices"}, 400

    job_id = uuid.uuid4().hex[:8]
    def worker():
        print(f"[OTA] job {job_id} → {len(dev_ids)} devices")
        ota = BulkOTA(str(fpath), dev_ids)
        ota.wait(timeout_s=None)                     # blocks here, not in Flask thread
        print(f"[OTA] job {job_id} done")

    threading.Thread(target=worker, daemon=True).start()
    return {"started": True, "job_id": job_id, "targets": dev_ids}, 202



"""
@app.route('/signup', methods=['GET','POST'])
def signup():
    if request.method == 'POST':
        fn = request.form.get('first_name','').strip()
        ln = request.form.get('last_name','').strip()
        email = request.form.get('email','').strip().lower()
        pw = request.form.get('password','')
        cpw = request.form.get('confirm_password','')
        sec = request.form.get('security_number','')
        
        # Validation
        if not all([fn,ln,email,pw,cpw,sec]):
            flash('All fields are required','error')
        elif sec != '1234':
            flash('Invalid signup security number','error')
        elif pw != cpw:
            flash('Passwords do not match','error')
        elif email in users:
            flash('Email already registered','error')
        else:
            # In real life, hash pw! Here store plain for demo
            users[email] = {'first':fn,'last':ln,'pw':pw}
            flash('Registration successful. Please log in.','success')
            return redirect(url_for('login'))
    return render_template('signup.html')
"""
"""
@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        fn    = request.form.get('first_name','').strip()
        ln    = request.form.get('last_name','').strip()
        email = request.form.get('email','').strip().lower()
        pw    = request.form.get('password','')
        cpw   = request.form.get('confirm_password','')
        sec   = request.form.get('security_number','')

        # basic validation
        if not all([fn, ln, email, pw, cpw, sec]):
            flash('All fields are required', 'error')
        elif sec != '1234':
            flash('Invalid signup security number', 'error')
        elif pw != cpw:
            flash('Passwords do not match', 'error')
        elif User.query.filter_by(email=email).first():
            flash('Email already registered', 'error')
        else:
            new_user = User(
                email   = email,
                first   = fn,
                last    = ln,
                pw_hash = generate_password_hash(pw)
            )
            db.session.add(new_user)
            db.session.commit()
            flash('Registration successful. Please log in.', 'success')
            return redirect(url_for('login'))

    return render_template('signup.html')
"""



@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        # ── grab and strip all form fields ─────────────────────
        fn       = request.form.get('first_name', '').strip()
        ln       = request.form.get('last_name',  '').strip()
        email    = request.form.get('email',      '').strip().lower()
        pw       = request.form.get('password',   '')
        cpw      = request.form.get('confirm_password', '')
        role     = request.form.get('role', 'user')          # "admin" | "user"
        sec_code = request.form.get('security_number', '')

        # ── validation ─────────────────────────────────────────
        if not all([fn, ln, email, pw, cpw]):
            flash('All fields are required', 'error')

        elif pw != cpw:
            flash('Passwords do not match', 'error')

        elif role == 'admin' and sec_code != '1234':
            flash('Invalid admin security code', 'error')

        elif User.query.filter_by(email=email).first():
            flash('Email already registered', 'error')

        else:
            # ── create user ───────────────────────────────────
            new_user = User(
                first   = fn,
                last    = ln,
                email   = email,
                pw_hash = generate_password_hash(pw),
                role    = role
            )
            db.session.add(new_user)
            db.session.commit()

            flash('Registration successful – please log in.', 'success')
            return redirect(url_for('login'))

        # fall-through → some validation error; re-show form

    # GET  or validation-error POST
    return render_template('signup.html')



"""
@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email','').strip().lower()
        pw = request.form.get('password','')
        user = users.get(email)
        if not user or user['pw'] != pw:
            flash('Invalid email/password','error')
        else:
            session['user_email'] = email
            return redirect(url_for('select_device'))
    return render_template('login.html')
"""

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email','').strip().lower()
        pw    = request.form.get('password','')
        user  = User.query.filter_by(email=email).first()

        if not user or not user.check_pw(pw):
            flash('Invalid email/password', 'error')
        else:
            session['user_email'] = email
            return redirect(url_for('select_device'))

    return render_template('login.html')

@app.route("/users")
@admin_required
def list_users():
    users = User.query.filter_by(role="user").all()
    return render_template("users.html", users=users)

@app.route("/users/delete/<int:uid>", methods=["POST"])
@admin_required
def delete_user(uid):
    to_del = User.query.get_or_404(uid)
    if to_del.role == "admin":
        flash("Cannot delete an admin account", "error")
    else:
        # also drop user-device links
        UserDevice.query.filter_by(user_id=uid).delete()
        db.session.delete(to_del)
        db.session.commit()
        flash("User deleted", "success")
    return redirect(url_for("list_users"))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.template_filter("ago")
def ago(seconds: int) -> str:
    """Render 75 → '1 min', 3 600 → '1 h', etc."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        mins = seconds // 60
        return f"{mins} min{'s' if mins > 1 else ''}"
    else:
        hrs = seconds // 3600
        return f"{hrs} h{'s' if hrs > 1 else ''}"


"""
# Protect your existing dashboard routes:
@app.before_request
def require_login():
    auth_paths = ['/login','/signup','/static/','/api/telemetry']
    if any(request.path.startswith(p) for p in auth_paths):
        return
    if 'user_email' not in session:
        return redirect(url_for('login'))
"""
@app.before_request
def require_login():
    auth_paths = ['/login', '/signup', '/static/', '/api/telemetry']
    if any(request.path.startswith(p) for p in auth_paths):
        return

    email = session.get('user_email')
    if not email:
        return redirect(url_for('login'))

    user = User.query.filter_by(email=email).first()

    if user is None:                # user was deleted
        session.clear()
        return redirect(url_for('login'))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

