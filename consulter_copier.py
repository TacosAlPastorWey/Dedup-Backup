import argparse
import os
import re
import shutil
import sys
import time
import traceback
from pathlib import Path

import psycopg2

try:
    from blake3 import blake3
except ModuleNotFoundError:
    blake3 = None

from explorer_hasher import (
    DEFAULT_QUEUE_DIR,
    build_queue_path,
    count_files,
    iter_hashed_files,
    load_hash_queue,
)

DEFAULT_DB_DSN = os.environ.get("CONSULTER_DB_DSN", "")
DEFAULT_DESTINATION_ROOT = Path(os.environ.get("CONSULTER_COPY_ROOT", "/home/uzr/data_full"))
COPY_CHUNK_SIZE = int(os.environ.get("CONSULTER_COPY_CHUNK_MB", "64")) * 1024 * 1024
DB_RETRIES = int(os.environ.get("CONSULTER_DB_RETRIES", "6"))
DB_RETRY_SLEEP = float(os.environ.get("CONSULTER_DB_RETRY_SLEEP", "0.75"))
FILENAME_MODES = {"hash", "first_name"}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS backup_disks (
    id BIGSERIAL PRIMARY KEY,
    label TEXT,
    mount_path TEXT UNIQUE NOT NULL,
    total_bytes BIGINT,
    free_bytes BIGINT,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS disks (
    id BIGSERIAL PRIMARY KEY,
    serial TEXT UNIQUE,
    model TEXT,
    label TEXT,
    filesystem TEXT,
    mount_path TEXT,
    size_bytes BIGINT,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    last_seen TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sessions (
    id BIGSERIAL PRIMARY KEY,
    disk_id BIGINT REFERENCES disks(id),
    status TEXT,
    started_at TIMESTAMP DEFAULT NOW(),
    finished_at TIMESTAMP,
    last_copied_hash TEXT,
    last_copied_path TEXT
);

CREATE TABLE IF NOT EXISTS files (
    id BIGSERIAL PRIMARY KEY,
    hash TEXT UNIQUE NOT NULL,
    size_bytes BIGINT,
    status TEXT,
    name TEXT,
    extension TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE files ADD COLUMN IF NOT EXISTS name TEXT;
ALTER TABLE files ADD COLUMN IF NOT EXISTS extension TEXT;

CREATE TABLE IF NOT EXISTS stored_files (
    id BIGSERIAL PRIMARY KEY,
    file_id BIGINT REFERENCES files(id),
    backup_disk_id BIGINT REFERENCES backup_disks(id),
    relative_path TEXT NOT NULL,
    verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_backup_disk_path UNIQUE (backup_disk_id, relative_path)
);

CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_disks_status ON disks(status);
CREATE INDEX IF NOT EXISTS idx_backup_disks_status ON backup_disks(status);
CREATE INDEX IF NOT EXISTS idx_stored_files_disk_file ON stored_files(backup_disk_id, file_id);
"""

RETRYABLE_DB_ERRORS = (
    psycopg2.errors.DeadlockDetected,
    psycopg2.errors.SerializationFailure,
    psycopg2.errors.LockNotAvailable,
)


def connect_db(dsn: str):
    if not dsn:
        raise ValueError(
            "Debe definir la conexión PostgreSQL. Use CONSULTER_DB_DSN o el argumento --db-dsn."
        )
    return psycopg2.connect(dsn)


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def run_db(conn, action: str, callback):
    for attempt in range(1, DB_RETRIES + 1):
        try:
            result = callback()
            if not conn.autocommit:
                conn.commit()
            return result
        except RETRYABLE_DB_ERRORS as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt >= DB_RETRIES:
                raise
            print(
                f"Reintentando DB {action} ({attempt}/{DB_RETRIES}) por bloqueo: {exc}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(DB_RETRY_SLEEP * attempt)


def get_or_create_backup_disk(conn, mount_path, label=None, total_bytes=None, free_bytes=None, status="online"):
    def query():
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO backup_disks (mount_path, label, total_bytes, free_bytes, status) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (mount_path) DO UPDATE SET "
                "label = COALESCE(EXCLUDED.label, backup_disks.label), "
                "total_bytes = COALESCE(EXCLUDED.total_bytes, backup_disks.total_bytes), "
                "free_bytes = COALESCE(EXCLUDED.free_bytes, backup_disks.free_bytes), "
                "status = EXCLUDED.status "
                "RETURNING id",
                (mount_path, label, total_bytes, free_bytes, status),
            )
            return cur.fetchone()[0]

    return run_db(conn, "backup_disks upsert", query)


def get_file_row(conn, hash_value):
    def query():
        with conn.cursor() as cur:
            cur.execute("SELECT id, status FROM files WHERE hash = %s", (hash_value,))
            return cur.fetchone()

    return run_db(conn, "files select", query)


def insert_reserved_file(conn, hash_value, file_meta):
    def query():
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO files (hash, size_bytes, status, name, extension) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (hash) DO NOTHING RETURNING id",
                (
                    hash_value,
                    file_meta.get("size_bytes"),
                    "reserved",
                    file_meta.get("name"),
                    file_meta.get("extension"),
                ),
            )
            row = cur.fetchone()
            return row[0] if row else None

    return run_db(conn, "files insert", query)


def mark_file_reserved(conn, file_id, file_meta):
    def query():
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE files SET "
                "status = %s, "
                "size_bytes = COALESCE(files.size_bytes, %s), "
                "name = COALESCE(files.name, %s), "
                "extension = COALESCE(files.extension, %s) "
                "WHERE id = %s",
                (
                    "reserved",
                    file_meta.get("size_bytes"),
                    file_meta.get("name"),
                    file_meta.get("extension"),
                    file_id,
                ),
            )

    run_db(conn, "files reserve", query)


def reserve_hash(conn, hash_value, file_meta):
    row = get_file_row(conn, hash_value)
    if row:
        file_id, status = row
        if status == "copied":
            return file_id, "copied", False
        mark_file_reserved(conn, file_id, file_meta)
        return file_id, "reserved", False

    file_id = insert_reserved_file(conn, hash_value, file_meta)
    if file_id:
        return file_id, "reserved", True

    row = get_file_row(conn, hash_value)
    if not row:
        raise RuntimeError(f"No se pudo reservar hash {hash_value}")

    file_id, status = row
    if status == "copied":
        return file_id, "copied", False
    mark_file_reserved(conn, file_id, file_meta)
    return file_id, "reserved", False


def update_file_status(conn, file_id, status):
    def query():
        with conn.cursor() as cur:
            cur.execute("UPDATE files SET status = %s WHERE id = %s", (status, file_id))

    run_db(conn, f"files status {status}", query)


def ensure_stored_file(conn, file_id, backup_disk_id, relative_path):
    def query():
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO stored_files (file_id, backup_disk_id, relative_path) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (backup_disk_id, relative_path) DO UPDATE SET "
                "file_id = EXCLUDED.file_id, verified = FALSE",
                (file_id, backup_disk_id, relative_path),
            )

    run_db(conn, "stored_files upsert", query)


def sanitize_filename(name: str, fallback: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|\x00-\x1f]+", "_", name.strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    return cleaned or fallback


def destination_for(destination_root: Path, digest: str, file_meta: dict, filename_mode: str = "hash") -> tuple[Path, bool]:
    """Determina la ruta de destino y retorna (path, was_renamed)"""
    extension = (file_meta.get("extension") or "").lower()
    extension_dir = extension.lstrip(".") or "no_ext"
    was_renamed = False

    if filename_mode == "first_name":
        safe_name = sanitize_filename(file_meta.get("name") or digest, digest)
        name_path = Path(safe_name)
        if extension and not name_path.suffix:
            safe_name = f"{safe_name}{extension}"
            name_path = Path(safe_name)
        content_dest = destination_root / extension_dir / safe_name
        
        # Si existe un archivo con el mismo nombre, usar sufijo file(n)
        if content_dest.exists():
            suffix = name_path.suffix or extension
            stem = name_path.stem or digest
            counter = 1
            while True:
                new_name = f"{stem}_file({counter}){suffix}"
                content_dest = destination_root / extension_dir / new_name
                if not content_dest.exists():
                    was_renamed = True
                    break
                counter += 1
                if counter > 10000:  # Evitar bucle infinito
                    raise RuntimeError(f"No se puede crear nombre unico para {stem}")
        
        return content_dest, was_renamed

    content_dest = destination_root / extension_dir / digest
    if extension:
        content_dest = content_dest.with_suffix(extension)
    return content_dest, was_renamed


def copy_file(source_path: str, destination_path: Path):
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination_path.with_name(f".{destination_path.name}.{os.getpid()}.part")
    try:
        with open(source_path, "rb") as source, tmp_path.open("wb") as destination:
            shutil.copyfileobj(source, destination, length=COPY_CHUNK_SIZE)
        shutil.copystat(source_path, tmp_path)
        os.replace(tmp_path, destination_path)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


def verify_file_hash(file_path: Path, expected_hash: str) -> bool:
    """Verifica que el hash BLAKE3 del archivo coincida con el esperado."""
    if blake3 is None:
        print(f"Advertencia: blake3 no disponible, omitiendo verificacion para {file_path}", file=sys.stderr, flush=True)
        return True
    
    try:
        hasher = blake3()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(COPY_CHUNK_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)
        computed_hash = hasher.hexdigest()
        return computed_hash == expected_hash
    except Exception as exc:
        print(f"Error verificando hash de {file_path}: {exc}", file=sys.stderr, flush=True)
        return False


def print_progress(disk_id: str, current: int, total: int):
    print(f"PROGRESS:{disk_id}:{current}:{total}", flush=True)


def count_queue_records(queue_path: str | Path) -> int:
    try:
        with Path(queue_path).open("r", encoding="utf-8") as queue_file:
            return sum(1 for line in queue_file if line.strip())
    except FileNotFoundError:
        return 0


def copy_one_file(
    conn,
    digest: str,
    file_meta: dict,
    backup_disk_id: int,
    destination_root: Path,
    summary: dict,
    filename_mode: str,
):
    summary["scanned"] += 1
    relative_path = file_meta.get("relative_path") or file_meta.get("name") or file_meta.get("path")
    file_id, status, created = reserve_hash(conn, digest, file_meta)

    if status == "copied":
        summary["already_registered"] += 1
        ensure_stored_file(conn, file_id, backup_disk_id, relative_path)
        return

    if created:
        summary["reserved"] += 1
    else:
        summary["retried"] += 1

    content_dest, was_renamed = destination_for(destination_root, digest, file_meta, filename_mode)
    try:
        expected_size = file_meta.get("size_bytes")
        if content_dest.exists() and (expected_size is None or content_dest.stat().st_size == expected_size):
            update_file_status(conn, file_id, "copied")
            ensure_stored_file(conn, file_id, backup_disk_id, relative_path)
            summary["copied_and_verified"] += 1
            return

        copy_file(file_meta["path"], content_dest)
        
        # Verificar integridad del archivo copiado
        if verify_file_hash(content_dest, digest):
            update_file_status(conn, file_id, "copied")
            ensure_stored_file(conn, file_id, backup_disk_id, relative_path)
            summary["copied_and_verified"] += 1
            if was_renamed:
                summary["renamed"] += 1
            print(f"Copiado y verificado {file_meta['path']} -> {content_dest}", flush=True)
        else:
            update_file_status(conn, file_id, "failed")
            ensure_stored_file(conn, file_id, backup_disk_id, relative_path)
            summary["failed"] += 1
            print(f"Error de integridad: hash no coincide para {content_dest}", file=sys.stderr, flush=True)
    except Exception as exc:
        update_file_status(conn, file_id, "failed")
        ensure_stored_file(conn, file_id, backup_disk_id, relative_path)
        summary["failed"] += 1
        print(f"Error copiando {file_meta.get('path')}: {exc}", file=sys.stderr, flush=True)


def copy_files(
    conn,
    records,
    backup_disk_id: int,
    destination_root: Path,
    disk_id: str,
    filename_mode: str = "hash",
    total_items: int = 0,
    emit_progress: bool = True,
):
    summary = {
        "scanned": 0,
        "already_registered": 0,
        "reserved": 0,
        "retried": 0,
        "copied_and_verified": 0,
        "renamed": 0,
        "failed": 0,
        "total_items": total_items,
    }
    if emit_progress:
        print_progress(disk_id, 0, total_items)

    progress_step = max(1, total_items // 100) if total_items else 25
    last_progress = 0
    for digest, file_meta in records:
        copy_one_file(conn, digest, file_meta, backup_disk_id, destination_root, summary, filename_mode)
        if emit_progress and summary["scanned"] - last_progress >= progress_step:
            print_progress(disk_id, summary["scanned"], total_items)
            last_progress = summary["scanned"]

    if emit_progress:
        final_total = total_items or summary["scanned"]
        print_progress(disk_id, final_total, final_total)
    return summary


def run_consulter(
    root: str,
    disk_id: str,
    destination_root: Path,
    conn,
    queue_path: str | Path | None = None,
    queue_dir: str | Path | None = None,
    from_queue: bool = False,
    include_system_dirs: bool = False,
    filename_mode: str = "hash",
):
    if filename_mode not in FILENAME_MODES:
        raise ValueError(f"filename_mode invalido: {filename_mode}")
    if not os.path.isdir(root):
        raise FileNotFoundError(f"La ruta de origen no existe o no es directorio: {root}")

    try:
        stats = os.statvfs(root)
        total_bytes = stats.f_blocks * stats.f_frsize
        free_bytes = stats.f_bavail * stats.f_frsize
    except OSError:
        total_bytes = None
        free_bytes = None

    backup_disk_id = get_or_create_backup_disk(
        conn,
        str(root),
        label=disk_id,
        total_bytes=total_bytes,
        free_bytes=free_bytes,
        status="online",
    )

    active_queue_path = Path(queue_path) if queue_path else build_queue_path(disk_id, queue_dir)
    if from_queue:
        if not queue_path:
            raise ValueError("--from-queue requiere --queue-path")
        total_items = count_queue_records(active_queue_path)
        print(f"Leyendo cola de hashes: {active_queue_path}", flush=True)
        records = load_hash_queue(active_queue_path)
        emit_copy_progress = True
    else:
        print("Contando archivos a explorar...", flush=True)
        total_items = count_files(root, include_system_dirs=include_system_dirs)
        print(f"Archivos a explorar: {total_items}", flush=True)
        print(f"Cola de hashes: {active_queue_path}", flush=True)
        progress_step = max(1, total_items // 100) if total_items else 1
        last_progress = {"value": -1}

        def hash_progress(current: int):
            if total_items <= 0:
                return
            if current == total_items or current - last_progress["value"] >= progress_step:
                print_progress(disk_id, current, total_items)
                last_progress["value"] = current

        records = iter_hashed_files(
            root,
            active_queue_path,
            include_system_dirs=include_system_dirs,
            progress_callback=hash_progress,
        )
        emit_copy_progress = False

    summary = copy_files(
        conn,
        records,
        backup_disk_id,
        destination_root,
        disk_id,
        filename_mode=filename_mode,
        total_items=total_items,
        emit_progress=emit_copy_progress,
    )
    if not from_queue and total_items:
        print_progress(disk_id, total_items, total_items)
    summary["queue_path"] = str(active_queue_path)
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Consulter Copier: hashea en streaming y copia solo los datos nuevos."
    )
    parser.add_argument(
        "root",
        nargs="?",
        help="Ruta montada del disco donde se hara el hashing.",
    )
    parser.add_argument(
        "--disk-id",
        default=None,
        help="Identificador del disco que se usa para registrar el escaneo en la base de datos.",
    )
    parser.add_argument(
        "--output-path",
        "--destination",
        dest="output_path",
        default=str(DEFAULT_DESTINATION_ROOT),
        help="Raiz donde se guardaran los archivos copiados.",
    )
    parser.add_argument(
        "--db-dsn",
        default=DEFAULT_DB_DSN,
        help="Cadena DSN para conectar a PostgreSQL. Tambien puede usarse CONSULTER_DB_DSN.",
    )
    parser.add_argument(
        "--filename-mode",
        choices=sorted(FILENAME_MODES),
        default=os.environ.get("CONSULTER_FILENAME_MODE", "hash"),
        help="Usa hash o el primer nombre confirmado por blake3 para nombrar el archivo copiado.",
    )
    parser.add_argument(
        "--queue-path",
        default=None,
        help="Cola JSONL a escribir o leer si se usa --from-queue.",
    )
    parser.add_argument(
        "--queue-dir",
        default=str(DEFAULT_QUEUE_DIR),
        help="Directorio donde se guardaran colas JSONL nuevas.",
    )
    parser.add_argument(
        "--from-queue",
        action="store_true",
        help="No hashea; copia desde una cola JSONL existente.",
    )
    parser.add_argument(
        "--include-system-dirs",
        action="store_true",
        help="Incluye carpetas de sistema como Windows, Program Files y ProgramData.",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Crea las tablas necesarias en la base de datos y termina.",
    )
    parser.add_argument(
        "--skip-schema-init",
        action="store_true",
        help="Evita inicializar el esquema en la base de datos para ejecuciones en subprocessos.",
    )
    return parser.parse_args(argv)


def print_summary(summary):
    print("Resumen:")
    print(f"  Archivos a explorar: {summary.get('total_items', 0)}")
    print(f"  Escaneados: {summary['scanned']}")
    print(f"  Ya registrados: {summary['already_registered']}")
    print(f"  Reservados para copia: {summary['reserved']}")
    print(f"  Reintentados: {summary['retried']}")
    print(f"  Copiados y verificados: {summary.get('copied_and_verified', 0)}")
    print(f"  Renombrados para evitar conflicto: {summary.get('renamed', 0)}")
    print(f"  Fallidos: {summary['failed']}")
    if summary.get("queue_path"):
        print(f"  Cola: {summary['queue_path']}")


def main(argv=None):
    args = parse_args() if argv is None else parse_args(argv)

    if not args.db_dsn:
        print(
            "Error: No se encontro DSN de PostgreSQL. Configure CONSULTER_DB_DSN o pase --db-dsn.",
            file=sys.stderr,
        )
        return 1

    conn = None
    try:
        conn = connect_db(args.db_dsn)
        if args.init_db:
            ensure_schema(conn)
            print("Esquema inicializado.")
            return 0

        if not args.root:
            print(
                "Error: La ruta de origen es obligatoria para ejecutar la copia. Use root.",
                file=sys.stderr,
            )
            return 1

        if not args.disk_id:
            args.disk_id = Path(args.root).name

        if not args.skip_schema_init:
            ensure_schema(conn)
        conn.autocommit = True

        destination_root = Path(args.output_path)
        destination_root.mkdir(parents=True, exist_ok=True)

        summary = run_consulter(
            args.root,
            args.disk_id,
            destination_root,
            conn,
            queue_path=args.queue_path,
            queue_dir=args.queue_dir,
            from_queue=args.from_queue,
            include_system_dirs=args.include_system_dirs,
            filename_mode=args.filename_mode,
        )
        print_summary(summary)
        return 0
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
