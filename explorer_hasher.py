import argparse
import json
import os
import sys
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from blake3 import blake3
except ModuleNotFoundError:
    print(
        "Error: falta instalar el paquete 'blake3' en este entorno.\n"
        "Activa tu virtualenv y ejecuta: pip install blake3",
        file=sys.stderr,
    )
    sys.exit(1)

EXTS = {
    # Documentos de Oficina y Paquetería
    ".doc", ".docx", ".docm", ".dot", ".dotx", ".xls", ".xlsx", ".xlsm", ".xlsb",
    ".xltx", ".csv", ".ppt", ".pptx", ".pps", ".ppsx", ".pdf", ".rtf", ".txt",
    ".odt", ".ods", ".odp", ".pages", ".numbers", ".key",

    # Comprimidos y Respaldos (Backups)
    ".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".iso", ".vmdk",
    ".ova", ".ovf", ".bak", ".old", ".backup",

    # Contabilidad Específica y Bases de Datos
    ".mdb", ".accdb", ".sql", ".sqlite", ".sqlite3", ".db", ".dbf", ".dat",
    ".xml", ".json", ".qbw", ".qbb",

    # Imágenes, Escaneos y Evidencias
    ".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp", ".svg", ".heic",

    # Multimedia (Clases, grabaciones, audios)
    ".mp4", ".mkv", ".mov", ".avi", ".wmv", ".flv", ".webm", ".mp3", ".wav",
    ".m4a", ".ogg",

    # Código Fuente y Scripts de Sistemas
    ".py", ".js", ".ts", ".php", ".java", ".class", ".jar", ".c", ".cpp", ".cs",
    ".html", ".htm", ".css", ".sh", ".bat", ".ps1",

    # Configuraciones y Llaves de Seguridad
    ".env", ".conf", ".config", ".ini", ".yaml", ".yml", ".pem", ".key", ".pub", ".ppk",

    # Diseño y Arquitectura (Planos de Redes/Edificios)
    ".psd", ".ai", ".indd", ".dwg", ".dxf", ".3ds", ".max",
}

EXTS = frozenset(EXTS)
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_QUEUE_DIR = Path(os.environ.get("CONSULTER_HASH_QUEUE_DIR", BASE_DIR / "hash_queues"))
CHUNK_SIZE = int(os.environ.get("CONSULTER_HASH_CHUNK_MB", "256")) * 1024 * 1024
HASH_THREADS = int(os.environ.get("CONSULTER_HASH_THREADS", "7"))
BATCH_SIZE = 1000
ROOT = "/media/uzr/CT UMF 23"
INCLUDE_SYSTEM_DIRS = os.environ.get("CONSULTER_INCLUDE_SYSTEM_DIRS", "").lower() in {
    "1",
    "true",
    "yes",
    "si",
}
SKIP_DIR_NAMES = frozenset(
    {
        "$recycle.bin",
        ".spotlight-v100",
        ".temporaryitems",
        ".trashes",
        "config.msi",
        "msocache",
        "program files",
        "program files (x86)",
        "programdata",
        "recovery",
        "system volume information",
        "windows",
        "windows.old",
        "windowsapps",
    }
)
SKIP_FILE_NAMES = frozenset({"desktop.ini", "thumbs.db"})
SKIP_FILE_PREFIXES = ("~$",)


def safe_queue_name(value: str) -> str:
    cleaned = "".join(char if char.isalnum() else "_" for char in value.strip())
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    return cleaned[:80] or "disk"


def build_queue_path(disk_id: str | None = None, queue_dir: str | Path | None = None) -> Path:
    queue_root = Path(queue_dir) if queue_dir else DEFAULT_QUEUE_DIR
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return queue_root / f"{safe_queue_name(disk_id or 'disk')}-{timestamp}.jsonl"


def should_skip_dir(name: str, include_system_dirs: bool | None = None) -> bool:
    include = INCLUDE_SYSTEM_DIRS if include_system_dirs is None else include_system_dirs
    return not include and name.strip().lower() in SKIP_DIR_NAMES


def should_skip_file(name: str) -> bool:
    lowered = name.strip().lower()
    return lowered in SKIP_FILE_NAMES or any(name.startswith(prefix) for prefix in SKIP_FILE_PREFIXES)


class HashQueueWriter:
    def __init__(self, queue_path: str | Path, root: str):
        self.queue_path = Path(queue_path)
        self.status_path = self.queue_path.with_name(self.queue_path.name + ".status.json")
        self.root = root
        self.file = None
        self.count = 0
        self.started_at = datetime.now(timezone.utc).isoformat()

    def __enter__(self):
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.queue_path.open("w", encoding="utf-8")
        self.write_status("running")
        return self

    def __exit__(self, exc_type, exc, traceback):
        if self.file:
            self.file.flush()
            self.file.close()
        self.write_status("failed" if exc_type else "done")

    def write_status(self, status: str):
        payload = {
            "status": status,
            "root": self.root,
            "queue_path": str(self.queue_path),
            "count": self.count,
            "hash_threads": HASH_THREADS,
            "chunk_size": CHUNK_SIZE,
            "started_at": self.started_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        tmp_path = self.status_path.with_name(self.status_path.name + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as status_file:
            json.dump(payload, status_file, indent=2, ensure_ascii=False)
        tmp_path.replace(self.status_path)

    def write(self, digest: str, file_meta: dict[str, Any]):
        if not self.file:
            return
        record = {"hash": digest, **file_meta}
        self.file.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
        self.file.write("\n")
        self.file.flush()
        self.count += 1
        if self.count % BATCH_SIZE == 0:
            self.write_status("running")


def iter_files(root: str, include_system_dirs: bool | None = None):
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if should_skip_dir(entry.name, include_system_dirs):
                                continue
                            stack.append(entry.path)
                            continue
                        if should_skip_file(entry.name):
                            continue
                        _, sep, ext = entry.name.rpartition(".")
                        if sep and ("." + ext.lower()) in EXTS:
                            if entry.is_file(follow_symlinks=False):
                                yield entry.path
                    except (PermissionError, FileNotFoundError, OSError):
                        continue
        except (PermissionError, FileNotFoundError, OSError):
            continue


def count_files(root: str, include_system_dirs: bool | None = None) -> int:
    return sum(1 for _path in iter_files(root, include_system_dirs=include_system_dirs))


def hash_file(path: str) -> str | None:
    try:
        if os.path.getsize(path) == 0:
            return None

        hasher = blake3(max_threads=HASH_THREADS)
        with open(path, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                hasher.update(chunk)
        return hasher.hexdigest()

    except (PermissionError, FileNotFoundError, OSError):
        return None


def process_batch(batch: list[str]) -> list[tuple[str, str | None]]:
    results = []
    for path in batch:
        try:
            results.append((path, hash_file(path)))
        except OSError as e:
            print(f"error reading {path}: {e}")
            results.append((path, None))
    return results


def build_file_meta(root: str, path: str, stat_result: os.stat_result) -> dict[str, Any]:
    return {
        "path": path,
        "name": os.path.basename(path),
        "relative_path": os.path.relpath(path, root),
        "size_bytes": stat_result.st_size,
        "modified_at": datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc).isoformat(),
        "extension": Path(path).suffix.lower(),
    }


def iter_hashed_files(
    root: str,
    queue_path: str | Path | None = None,
    include_system_dirs: bool | None = None,
    progress_callback=None,
) -> Iterable[tuple[str, dict[str, Any]]]:
    writer_context = HashQueueWriter(queue_path, root) if queue_path else nullcontext(None)
    with writer_context as writer:
        scanned = 0
        for path in iter_files(root, include_system_dirs=include_system_dirs):
            scanned += 1
            digest = hash_file(path)
            if progress_callback:
                progress_callback(scanned)
            if not digest:
                continue

            try:
                stat = os.stat(path)
            except (OSError, PermissionError):
                continue

            file_meta = build_file_meta(root, path, stat)
            if writer:
                writer.write(digest, file_meta)
            yield digest, file_meta


def load_hash_queue(queue_path: str | Path) -> Iterable[tuple[str, dict[str, Any]]]:
    with Path(queue_path).open("r", encoding="utf-8") as queue_file:
        for line_number, line in enumerate(queue_file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"Registro de cola invalido en linea {line_number}: {exc}", file=sys.stderr)
                continue
            digest = record.pop("hash", None)
            if not digest:
                print(f"Registro de cola sin hash en linea {line_number}", file=sys.stderr)
                continue
            yield digest, record


def hash_files(root: str, include_system_dirs: bool | None = None) -> dict[str, dict[str, Any]]:
    hashes: dict[str, dict[str, Any]] = {}
    for digest, file_meta in iter_hashed_files(root, include_system_dirs=include_system_dirs):
        if digest in hashes:
            continue
        hashes[digest] = file_meta
    return dict(sorted(hashes.items(), key=lambda item: item[1]["path"]))


def write_hash_queue(
    root: str,
    queue_path: str | Path | None = None,
    disk_id: str | None = None,
    include_system_dirs: bool | None = None,
) -> Path:
    destination = Path(queue_path) if queue_path else build_queue_path(disk_id or Path(root).name)
    for _digest, _file_meta in iter_hashed_files(root, destination, include_system_dirs=include_system_dirs):
        pass
    return destination


def run(root: str) -> dict[str, dict[str, Any]]:
    return hash_files(root)


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Hasher incremental de discos.")
    parser.add_argument("root", help="Ruta montada del disco a explorar.")
    parser.add_argument("--queue-path", help="Archivo JSONL donde se escribira la cola de hashes.")
    parser.add_argument("--queue-dir", default=str(DEFAULT_QUEUE_DIR), help="Directorio para colas JSONL.")
    parser.add_argument("--disk-id", help="Identificador usado para nombrar la cola.")
    parser.add_argument("--no-json", action="store_true", help="No imprime el JSON completo al terminar.")
    parser.add_argument(
        "--include-system-dirs",
        action="store_true",
        help="Incluye carpetas de sistema como Windows, Program Files y ProgramData.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ruta_objetivo = args.root

    if not os.path.exists(ruta_objetivo):
        print(f"Error: La ruta '{ruta_objetivo}' no existe o no es accesible.")
        return 1

    if not os.path.isdir(ruta_objetivo):
        print(f"Error: La ruta '{ruta_objetivo}' no es un directorio.")
        return 1

    queue_path = None
    if args.queue_path:
        queue_path = Path(args.queue_path)
    elif args.no_json or args.disk_id:
        queue_path = build_queue_path(args.disk_id or Path(ruta_objetivo).name, args.queue_dir)

    if args.no_json:
        count = 0
        for count, (_digest, _file_meta) in enumerate(
            iter_hashed_files(ruta_objetivo, queue_path, include_system_dirs=args.include_system_dirs),
            start=1,
        ):
            if count % BATCH_SIZE == 0:
                print(f"hashed {count} files...", flush=True)
        if queue_path:
            print(f"queue: {queue_path}")
        print(f"done. {count} files hashed.")
        return 0

    hashes: dict[str, dict[str, Any]] = {}
    for digest, file_meta in iter_hashed_files(
        ruta_objetivo,
        queue_path,
        include_system_dirs=args.include_system_dirs,
    ):
        if digest not in hashes:
            hashes[digest] = file_meta
    hashes = dict(sorted(hashes.items(), key=lambda item: item[1]["path"]))
    print(json.dumps(hashes, indent=2, ensure_ascii=False))
    if queue_path:
        print(f"queue: {queue_path}")
    print(f"done. {len(hashes)} files hashed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
