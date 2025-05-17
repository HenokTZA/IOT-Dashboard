# fix.py
#
# One-time migration: convert any naïve Device.last_seen timestamps
# to timezone-aware UTC.  Run once, then delete the file.

from datetime import timezone
from app import app, db, Device               # import your app + model

def main():
    fixed = 0
    with app.app_context():                   # <-- key addition
        for d in Device.query.all():
            if d.last_seen and d.last_seen.tzinfo is None:
                d.last_seen = d.last_seen.replace(tzinfo=timezone.utc)
                fixed += 1
        if fixed:
            db.session.commit()
            print(f"✅ Patched {fixed} rows to UTC-aware")
        else:
            print("🎉 All rows already timezone-aware")

if __name__ == "__main__":
    main()


