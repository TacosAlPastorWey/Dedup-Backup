import getpass
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "backup_profiles.json"
COPIER_SCRIPT = BASE_DIR / "consulter_copier.py"
MOUNT_BASE = Path("/media") / getpass.getuser()
DEFAULT_COPY_ROOT = Path(os.environ.get("CONSULTER_COPY_ROOT", "/home/uzr/data_full"))
DEFAULT_PERSONAL_COPY_ROOT = Path(os.environ.get("CONSULTER_PERSONAL_COPY_ROOT", "/home/uzr/data_full_personal"))
DEFAULT_WORKERS = int(os.environ.get("CONSULTER_HASH_THREADS", "7"))
PYTHON_BIN = Path(os.environ.get("CONSULTER_PYTHON", "/home/uzr/v_envs/Scrapper/bin/python"))
if not PYTHON_BIN.exists():
    PYTHON_BIN = Path(sys.executable)


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def dsn_from_env(db_name: str | None = None) -> str:
    env_file = read_env_file(BASE_DIR / ".env")
    existing = os.environ.get("CONSULTER_DB_DSN")
    if existing:
        return replace_db_name(existing, db_name) if db_name else existing

    name = db_name or env_file.get("DB_NAME") or "scrapper"
    parts = [f"dbname={name}"]
    if env_file.get("DB_USER"):
        parts.append(f"user={env_file['DB_USER']}")
    if env_file.get("DB_PASS"):
        parts.append(f"password={env_file['DB_PASS']}")
    if env_file.get("DB_HOST"):
        parts.append(f"host={env_file['DB_HOST']}")
    if env_file.get("DB_PORT"):
        parts.append(f"port={env_file['DB_PORT']}")
    return " ".join(parts)


def replace_db_name(dsn: str, db_name: str | None) -> str:
    if not db_name:
        return dsn
    parts = shlex.split(dsn)
    replaced = False
    for index, part in enumerate(parts):
        if part.startswith("dbname="):
            parts[index] = f"dbname={db_name}"
            replaced = True
            break
    if not replaced:
        parts.insert(0, f"dbname={db_name}")
    return " ".join(parts)


def default_config() -> dict:
    return {
        "active_profile": "resistencia",
        "filename_mode": "hash",
        "profiles": {
            "resistencia": {
                "db_dsn": dsn_from_env(),
                "copy_root": str(DEFAULT_COPY_ROOT),
            },
            "personal": {
                "db_dsn": dsn_from_env("personal"),
                "copy_root": str(DEFAULT_PERSONAL_COPY_ROOT),
            },
        },
    }


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        config = default_config()
        save_config(config)
        return config
    try:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        config = default_config()
    defaults = default_config()
    config.setdefault("active_profile", defaults["active_profile"])
    config.setdefault("filename_mode", defaults["filename_mode"])
    config.setdefault("profiles", {})
    for name, profile in defaults["profiles"].items():
        config["profiles"].setdefault(name, profile)
    save_config(config)
    return config


def save_config(config: dict):
    tmp_path = CONFIG_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp_path.replace(CONFIG_PATH)


def prompt(default_prompt: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default not in (None, "") else ""
    try:
        value = input(f"{default_prompt}{suffix}: ").strip()
    except EOFError:
        return default or ""
    return value if value else (default or "")


def prompt_int(label: str, default: int, minimum: int = 1) -> int:
    while True:
        value = prompt(label, str(default))
        try:
            parsed = int(value)
        except ValueError:
            print("Ingresa un numero valido.")
            continue
        if parsed < minimum:
            print(f"Debe ser mayor o igual a {minimum}.")
            continue
        return parsed


def active_profile(config: dict) -> tuple[str, dict]:
    name = config.get("active_profile") or "resistencia"
    profiles = config.setdefault("profiles", {})
    if name not in profiles:
        profiles[name] = default_config()["profiles"]["resistencia"]
    return name, profiles[name]


def configure(config: dict):
    print("\nConfigurar")
    print("Perfiles disponibles:")
    for name in sorted(config["profiles"]):
        marker = "*" if name == config.get("active_profile") else " "
        print(f" {marker} {name}")

    profile_name = prompt("Perfil a editar", config.get("active_profile", "resistencia"))
    profile = config["profiles"].setdefault(
        profile_name,
        {"db_dsn": dsn_from_env(profile_name if profile_name == "personal" else None), "copy_root": str(DEFAULT_COPY_ROOT)},
    )
    profile["db_dsn"] = prompt("Ubicacion/DSN de base de datos", profile.get("db_dsn", ""))
    profile["copy_root"] = prompt("Carpeta donde se copian archivos", profile.get("copy_root", str(DEFAULT_COPY_ROOT)))

    activate = prompt("Usar este perfil como activo? (s/n)", "s" if profile_name == config.get("active_profile") else "n")
    if activate.lower().startswith("s"):
        config["active_profile"] = profile_name

    print("\nModo de nombre de archivo:")
    print(" 1. hash blake3")
    print(" 2. primer nombre confirmado por blake3")
    current_mode = config.get("filename_mode", "hash")
    default_choice = "2" if current_mode == "first_name" else "1"
    choice = prompt("Elige modo", default_choice)
    config["filename_mode"] = "first_name" if choice == "2" else "hash"

    save_config(config)
    print(f"Configuracion guardada en {CONFIG_PATH}\n")


def run_lsblk() -> list[dict]:
    command = [
        "lsblk",
        "-J",
        "-o",
        "PATH,NAME,TYPE,SIZE,LABEL,FSTYPE,MOUNTPOINT,TRAN,MODEL",
    ]
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr.strip() or "No se pudo ejecutar lsblk.")
        return []
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return []

    disks: list[dict] = []

    def walk(node: dict, parent: dict | None = None):
        inherited = parent or {}
        merged = {
            "path": node.get("path"),
            "name": node.get("name"),
            "type": node.get("type"),
            "size": node.get("size"),
            "label": node.get("label"),
            "fstype": node.get("fstype"),
            "mountpoint": node.get("mountpoint"),
            "tran": node.get("tran") or inherited.get("tran"),
            "model": node.get("model") or inherited.get("model"),
        }
        if merged["type"] == "part" and merged["fstype"]:
            disks.append(merged)
        for child in node.get("children") or []:
            walk(child, merged)

    for block in payload.get("blockdevices") or []:
        walk(block)
    return disks


def disk_label(entry: dict) -> str:
    label = entry.get("label") or entry.get("model") or entry.get("name") or entry.get("path")
    bits = [label]
    if entry.get("size"):
        bits.append(entry["size"])
    if entry.get("fstype"):
        bits.append(entry["fstype"])
    if entry.get("mountpoint"):
        bits.append(f"montado: {entry['mountpoint']}")
    else:
        bits.append("sin montar")
    return " | ".join(bits)


def mount_entry(entry: dict) -> str | None:
    if entry.get("mountpoint"):
        return entry["mountpoint"]
    device_path = entry.get("path")
    if not device_path:
        return None
    result = subprocess.run(["udisksctl", "mount", "-b", device_path], capture_output=True, text=True)
    output = (result.stdout or "") + (result.stderr or "")
    if result.returncode != 0:
        print(output.strip() or f"No se pudo montar {device_path}.")
        return None
    match = re.search(r" at (.+?)\.?$", output.strip())
    if match:
        return match.group(1)
    for refreshed in run_lsblk():
        if refreshed.get("path") == device_path and refreshed.get("mountpoint"):
            return refreshed["mountpoint"]
    return None


def find_mountpoint(path: Path) -> Path:
    current = path.resolve()
    while current != current.parent:
        if os.path.ismount(current):
            return current
        current = current.parent
    return current


def validate_manual_path(raw_path: str) -> tuple[bool, Path | None, str]:
    if not raw_path.strip():
        return False, None, "No se ingreso ruta."
    path = Path(raw_path).expanduser()
    if not path.exists():
        return False, path, "La carpeta no existe o no esta montada."
    if not path.is_dir():
        return False, path, "La ruta existe, pero no es una carpeta."
    try:
        mountpoint = find_mountpoint(path)
    except OSError:
        return False, path, "La carpeta no es accesible o no esta montada."
    if str(path).startswith(str(MOUNT_BASE)) and mountpoint == Path("/"):
        return False, path, "La carpeta parece estar bajo /media, pero no esta montada."
    return True, path, ""


def validate_manual_file(raw_path: str) -> tuple[bool, Path | None, str]:
    if not raw_path.strip():
        return False, None, "No se ingreso ruta."
    path = Path(raw_path).expanduser()
    if not path.exists():
        return False, path, "El archivo no existe o no esta accesible."
    if not path.is_file():
        return False, path, "La ruta existe, pero no es un archivo."
    try:
        with open(path, "rb") as f:
            f.read(1)
    except OSError as e:
        return False, path, f"El archivo no es legible: {e}"
    return True, path, ""


def render_progress(current: int, total: int, width: int = 34):
    if total <= 0:
        sys.stdout.write(f"\rProcesados {current} archivos...")
        sys.stdout.flush()
        return
    ratio = min(max(current / total, 0.0), 1.0)
    filled = int(ratio * width)
    bar = "#" * filled + "-" * (width - filled)
    percent = int(ratio * 100)
    sys.stdout.write(f"\r[{bar}] {percent:3d}% ({current}/{total})")
    sys.stdout.flush()


def run_copier_for_path(config: dict, source_path: Path, disk_id: str):
    profile_name, profile = active_profile(config)
    workers = prompt_int("Workers para blake3", DEFAULT_WORKERS)
    copy_root = Path(profile["copy_root"]).expanduser()
    copy_root.mkdir(parents=True, exist_ok=True)

    command = [
        str(PYTHON_BIN),
        str(COPIER_SCRIPT),
        str(source_path),
        "--disk-id",
        disk_id,
        "--output-path",
        str(copy_root),
        "--db-dsn",
        profile["db_dsn"],
        "--filename-mode",
        config.get("filename_mode", "hash"),
    ]
    env = os.environ.copy()
    env["CONSULTER_HASH_THREADS"] = str(workers)
    env["PYTHONUNBUFFERED"] = "1"

    print(f"\nPerfil: {profile_name}")
    print(f"Origen: {source_path}")
    print(f"Destino: {copy_root}")
    print(f"Modo nombre: {config.get('filename_mode', 'hash')}")
    print(f"Workers blake3: {workers}\n")

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1,
    )
    progress_active = False
    assert process.stdout is not None
    for raw_line in process.stdout:
        line = raw_line.rstrip("\n")
        if line.startswith("PROGRESS:"):
            parts = line.split(":")
            if len(parts) == 4:
                try:
                    render_progress(int(parts[2]), int(parts[3]))
                    progress_active = True
                except ValueError:
                    pass
            continue
        if progress_active:
            print()
            progress_active = False
        print(line)
    if progress_active:
        print()
    exit_code = process.wait()
    print(f"Proceso terminado con codigo {exit_code}\n")


def choose_disk(config: dict):
    disks = run_lsblk()
    external = [disk for disk in disks if disk.get("path") and disk.get("path") not in {"/dev/nvme0n1p1", "/dev/nvme0n1p2"}]
    if not external:
        print("No se encontraron discos/particiones externas con filesystem.")
        return
    print("\nDiscos/particiones detectadas:")
    for index, entry in enumerate(external, start=1):
        print(f" {index}. {disk_label(entry)}")
    choice = prompt_int("Elige disco", 1)
    if choice < 1 or choice > len(external):
        print("Opcion fuera de rango.")
        return
    entry = external[choice - 1]
    mountpoint = mount_entry(entry)
    if not mountpoint:
        print("No se pudo montar o localizar el disco elegido.")
        return
    label = entry.get("label") or entry.get("model") or entry.get("name") or Path(mountpoint).name
    run_copier_for_path(config, Path(mountpoint), label)


def manual_path(config: dict):
    raw_path = prompt("Ruta de carpeta a explorar")
    ok, path, message = validate_manual_path(raw_path)
    if not ok:
        print(message)
        return
    assert path is not None
    disk_id = prompt("Nombre para esta ejecucion", path.name or "manual")
    run_copier_for_path(config, path, disk_id)


def manual_file(config: dict):
    import tempfile
    raw_path = prompt("Ruta del archivo a respaldar")
    ok, path, message = validate_manual_file(raw_path)
    if not ok:
        print(message)
        return
    assert path is not None
    disk_id = prompt("Nombre para esta ejecucion", path.stem or "archivo")

    # Crear directorio temporal para el archivo
    temp_dir = Path(tempfile.mkdtemp(prefix="consulter_file_"))
    try:
        # Crear enlace simbólico al archivo en el directorio temporal
        temp_file = temp_dir / path.name
        temp_file.symlink_to(path)
        print(f"Archivo enlazado en: {temp_dir}")
        run_copier_for_path(config, temp_dir, disk_id)
    finally:
        # Limpiar directorio temporal
        try:
            for item in temp_dir.iterdir():
                if item.is_symlink() or item.is_file():
                    item.unlink()
            temp_dir.rmdir()
        except Exception as e:
            print(f"Advertencia: No se pudo limpiar {temp_dir}: {e}")


def main():
    config = load_config()
    while True:
        profile_name, profile = active_profile(config)
        print("\nMenu de respaldo")
        print(f"Perfil activo: {profile_name}")
        print(f"Destino: {profile.get('copy_root')}")
        print(f"Modo nombre: {config.get('filename_mode', 'hash')}")
        print(" 1. Iniciar desde disco conectado")
        print(" 2. Iniciar desde una carpeta/ruta")
        print(" 3. Iniciar desde un archivo individual")
        print(" 4. Configurar")
        print(" q. Salir")
        try:
            choice = input("Opcion: ").strip().lower()
        except EOFError:
            print()
            break
        if choice == "1":
            choose_disk(config)
        elif choice == "2":
            manual_path(config)
        elif choice == "3":
            manual_file(config)
        elif choice == "4":
            configure(config)
        elif choice in {"q", "quit", "salir"}:
            break
        else:
            print("Opcion no valida.")


if __name__ == "__main__":
    main()
