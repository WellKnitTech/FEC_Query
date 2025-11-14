# Database Corruption Recovery Instructions

## Current Situation
The SQLite database (`fec_data.db`) is severely corrupted and cannot be read. The integrity check shows multiple corrupted pages with error code 522.

## Immediate Actions Required

### Option 1: Start Fresh (Recommended if data can be re-imported)
If you can re-import data from the FEC API:

1. **Stop the application** (Ctrl+C or kill processes)
2. **Backup the corrupted database** (already done: `fec_data.db.backup.20251112_172451`)
3. **Remove corrupted database files**:
   ```bash
   cd backend
   rm fec_data.db fec_data.db-wal fec_data.db-shm
   ```
4. **Restart the application** - it will create a new empty database
5. **Re-import data** using the bulk data import features

### Option 2: Check Disk Health
The corruption might indicate disk issues:

```bash
# Check disk health (read-only check)
sudo smartctl -a /dev/nvme1n1p2

# Check filesystem
sudo fsck -n /dev/nvme1n1p2
```

### Option 3: Try SQLite Recovery Tools
If you have critical data:

```bash
# Install sqlite3 recovery tools
sudo pacman -S sqlite

# Try to dump what's recoverable
sqlite3 fec_data.db ".recover" | sqlite3 fec_data_recovered.db
```

## Prevention Measures

The corruption might be caused by:
1. **Concurrent access issues** - Already addressed with WAL mode
2. **Disk I/O errors** - Check disk health
3. **Power loss during writes** - Ensure proper shutdown
4. **File system issues** - Check filesystem integrity

## Database Configuration
The current configuration includes:
- WAL mode enabled
- WAL autocheckpoint every 1000 pages
- Synchronous=NORMAL
- Busy timeout of 60 seconds

Consider adding:
- More frequent WAL checkpoints
- Periodic integrity checks
- Database backups

## Next Steps
1. Stop the application
2. Choose recovery option above
3. Restart application
4. Monitor for recurring corruption

