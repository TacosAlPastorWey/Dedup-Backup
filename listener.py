import argparse
import getpass
import hashlib
import os
import json
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psutil
import pyudev
from loguru import logger


BASE_DIR = Path(__file__).resolve().parent
REGISTRY_PATH = BASE_DIR / "discos_registrados.json"
LOG_PATH = BASE_DIR / "disk_listener.log"
MOUNT_BASE = Path("/media") / getpass.getuser()
AUTO_MOUNT_WAIT_SECONDS = 8
UDISKSCTL_TIMEOUT_SECONDS = 25
MANUAL_MOUNT_TIMEOUT_SECONDS = 20
PARTITION_APPEAR_WAIT_SECONDS = 12
DEFAULT_COPY_ROOT = Path(os.environ.get("CONSULTER_COPY_ROOT", "/home/uzr/data_full"))
DEFAULT_PYTHON = Path(os.environ.get("PYTHON", sys.executable))
DEFAULT_CONSULTER_DB_DSN = os.environ.get("CONSULTER_DB_DSN", "")

logger.add(LOG_PATH, rotation="10 MB", level="INFO")


class DiskListener:
    def __init__(self):
        self.context = pyudev.Context()
        self.monitor = pyudev.Monitor.from_netlink(self.context)
        self.monitor.filter_by(subsystem="block")
        self.monitor.start()
        self.registry_lock = threading.Lock()
        self.active_lock = threading.Lock()
        self.processing_lock = threading.Lock()
        self.process_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        self.processing_devices = set()
        self.registry = self.cargar_registro()
        self.active_devices = {}
        self.python_path = DEFAULT_PYTHON
        self.consulter_script = BASE_DIR / "consulter_copier.py"
        self.consulter_db_dsn = DEFAULT_CONSULTER_DB_DSN
        self.output_root = DEFAULT_COPY_ROOT
        self.disk_processes = {}
        self.progress_status = {}
        self.process_log_dir = BASE_DIR / "disk_listener_logs"
        self.process_log_dir.mkdir(parents=True, exist_ok=True)

    def cargar_registro(self):
        if not REGISTRY_PATH.exists():
            return {}

        try:
            with REGISTRY_PATH.open("r", encoding="utf-8") as file:
                registry = json.load(file)
                return self.consolidar_registro(registry)
        except Exception as exc:
            logger.error(f"No se pudo leer el registro {REGISTRY_PATH}: {exc}")
            return {}

    def consolidar_registro(self, registry):
        consolidated = {}
        identity_index = {}

        for disk_id, info in registry.items():
            identities = self.identidades_info(info)
            target_id = None
            for identity in identities:
                if identity in identity_index:
                    target_id = identity_index[identity]
                    break

            if not target_id:
                target_id = info.get("id") or disk_id
                consolidated[target_id] = info
                consolidated[target_id]["id"] = target_id
            else:
                current = consolidated[target_id]
                current.update({key: value for key, value in info.items() if value})
                current["id"] = target_id
                first_current = current.get("primer_registro")
                first_new = info.get("primer_registro")
                if first_current and first_new:
                    current["primer_registro"] = min(first_current, first_new)
                elif first_new:
                    current["primer_registro"] = first_new

            for identity in identities:
                identity_index[identity] = target_id

        return consolidated

    def guardar_registro(self):
        tmp_path = REGISTRY_PATH.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as file:
            json.dump(self.registry, file, indent=2, ensure_ascii=False, sort_keys=True)
        tmp_path.replace(REGISTRY_PATH)

    def initialize_db_schema(self):
        if not self.consulter_script.exists() or not self.consulter_db_dsn:
            return False

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        command = [
            str(self.python_path),
            str(self.consulter_script),
            "--db-dsn",
            self.consulter_db_dsn,
            "--init-db",
        ]

        try:
            subprocess.run(
                command,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=env,
                text=True,
            )
            return True
        except subprocess.CalledProcessError as exc:
            self.avisar(
                f"Error inicializando esquema en PostgreSQL: {exc.stderr.strip()}",
                "error",
            )
            return False

    def refrescar_device(self, device, delay=0.5):
        device_node = device.device_node
        if not device_node:
            return device

        time.sleep(delay)
        try:
            return pyudev.Devices.from_device_file(self.context, device_node)
        except Exception:
            return device

    def es_particion_montable(self, device):
        if device.get("DEVTYPE") not in {"disk", "partition"}:
            return False

        fs_usage = device.get("ID_FS_USAGE")
        fs_type = device.get("ID_FS_TYPE")
        if fs_usage == "filesystem" or fs_type:
            return True

        logger.info(
            f"Ignorando {device.device_node or device.sys_name}: "
            "no tiene filesystem montable."
        )
        return False

    def limpiar_nombre_montaje(self, value):
        cleaned = value.replace("\\x20", " ").strip()
        cleaned = re.sub(r"[^A-Za-z0-9._ -]+", "_", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned or "DISK"

    def decodificar_findmnt(self, value):
        return re.sub(
            r"\\x([0-9A-Fa-f]{2})",
            lambda match: chr(int(match.group(1), 16)),
            value,
        )

    def crear_punto_montaje_unico(self, mount_name):
        base = MOUNT_BASE / mount_name
        if not base.exists() or not any(base.iterdir()):
            return base

        for index in range(2, 100):
            candidate = MOUNT_BASE / f"{mount_name}-{index}"
            if not candidate.exists() or not any(candidate.iterdir()):
                return candidate

        return MOUNT_BASE / f"{mount_name}-{int(time.time())}"

    def avisar(self, mensaje, level="info"):
        print(mensaje, flush=True)
        log_method = getattr(logger, level, logger.info)
        log_method(mensaje)

    def ejecutar(self, comando, timeout=20):
        try:
            return subprocess.run(
                comando,
                capture_output=True,
                check=False,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            self.avisar(f"Timeout ejecutando: {' '.join(comando)}", "warning")
        except FileNotFoundError:
            self.avisar(f"Comando no encontrado: {comando[0]}", "warning")
        except Exception as exc:
            self.avisar(f"Error ejecutando {' '.join(comando)}: {exc}", "error")

        return None

    def obtener_padre_disco(self, device):
        if device.get("DEVTYPE") == "disk":
            return device

        try:
            parent = device.find_parent("block", "disk")
            if parent is not None:
                return parent
        except Exception:
            pass

        current = device
        while current is not None:
            if current.get("DEVTYPE") == "disk":
                return current
            current = current.parent
        return device

    def obtener_propiedad(self, device, keys):
        for key in keys:
            value = device.get(key)
            if value:
                return value
        return None

    def obtener_nombre(self, device, disk_device=None):
        if disk_device is None:
            disk_device = self.obtener_padre_disco(device)
        label = device.get("ID_FS_LABEL") or device.get("ID_FS_LABEL_ENC")
        model = disk_device.get("ID_MODEL") or disk_device.get("ID_MODEL_ENC")
        vendor = disk_device.get("ID_VENDOR") or disk_device.get("ID_VENDOR_ENC")
        serial = disk_device.get("ID_SERIAL_SHORT") or disk_device.get("ID_SERIAL")

        parts = [value for value in (vendor, model) if value]
        if label:
            return label.replace("\\x20", " ")
        if parts:
            return " ".join(parts).replace("_", " ").strip()
        if serial:
            return f"Disco {serial}"
        return device.device_node or device.sys_name

    def particiones_del_disco(self, disk_device):
        partitions = []
        for partition in self.context.list_devices(subsystem="block", DEVTYPE="partition"):
            try:
                parent = partition.find_parent("block", "disk")
            except Exception:
                parent = None
            if parent is not None and parent.sys_name == disk_device.sys_name:
                partitions.append(partition)
        return sorted(partitions, key=lambda item: item.sys_name)

    def esperar_particiones_montables(self, disk_device, segundos=PARTITION_APPEAR_WAIT_SECONDS):
        for elapsed in range(segundos):
            partitions = self.particiones_del_disco(disk_device)
            mountable = [partition for partition in partitions if self.es_particion_montable(partition)]
            if mountable:
                return mountable

            if elapsed in {0, 4, 9}:
                self.avisar(
                    f"Esperando particiones montables de {disk_device.device_node or disk_device.sys_name}... "
                    f"{elapsed + 1}/{segundos}s"
                )
            time.sleep(1)
        return []

    def reintentar_lectura_particiones(self, disk_device):
        disk_node = disk_device.device_node
        if not disk_node:
            return

        self.avisar(f"No aparecieron particiones para {disk_node}; intentando releer tabla...")
        self.ejecutar(["udevadm", "settle"], timeout=8)

        for comando in (["partprobe", disk_node], ["blockdev", "--rereadpt", disk_node]):
            result = self.ejecutar(comando, timeout=8)
            if result and result.returncode == 0:
                self.avisar(f"Comando exitoso: {' '.join(comando)}")
                self.ejecutar(["udevadm", "settle"], timeout=8)
                return
            if result and result.stderr.strip():
                self.avisar(
                    f"No se pudo ejecutar {' '.join(comando)}: {result.stderr.strip()}",
                    "warning",
                )

    def uuid_tabla_particiones(self, disk_device, partitions):
        table_uuid = disk_device.get("ID_PART_TABLE_UUID")
        if table_uuid:
            return table_uuid

        for partition in partitions:
            table_uuid = partition.get("ID_PART_TABLE_UUID")
            if table_uuid:
                return table_uuid
        return None

    def uuids_particiones(self, partitions):
        values = []
        for partition in partitions:
            part_uuid = partition.get("ID_PART_ENTRY_UUID")
            fs_uuid = partition.get("ID_FS_UUID")
            if part_uuid:
                values.append(f"partuuid:{part_uuid}")
            if fs_uuid:
                values.append(f"fsuuid:{fs_uuid}")
        return sorted(set(values))

    def obtener_huella_disco(self, device):
        disk_device = self.obtener_padre_disco(device)
        partitions = self.particiones_del_disco(disk_device)
        serial = self.obtener_propiedad(disk_device, ["ID_SERIAL", "ID_SERIAL_SHORT"])
        wwn = disk_device.get("ID_WWN")
        table_uuid = self.uuid_tabla_particiones(disk_device, partitions)
        partition_uuids = self.uuids_particiones(partitions)

        if wwn:
            return "wwn", wwn
        if table_uuid:
            return "partition_table_uuid", table_uuid
        if partition_uuids:
            return "partition_uuids", "|".join(partition_uuids)
        if serial:
            return "serial", serial

        fallback_values = [
            disk_device.get("ID_BUS"),
            disk_device.get("ID_VENDOR"),
            disk_device.get("ID_MODEL"),
            disk_device.get("ID_REVISION"),
            disk_device.get("size"),
        ]
        raw = "|".join(value for value in fallback_values if value)
        if raw:
            return "weak_hardware_fallback", raw

        return "temporary_kernel_path", disk_device.device_path or disk_device.sys_name

    def crear_id_disco(self, device):
        source, raw = self.obtener_huella_disco(device)
        digest = hashlib.sha256(f"{source}:{raw}".encode("utf-8")).hexdigest()[:16]
        return f"disk-{digest}"

    def huella_es_fuerte(self, source):
        return source in {"wwn", "partition_table_uuid", "partition_uuids", "serial"}

    def obtener_info_disco(self, device):
        disk_device = self.obtener_padre_disco(device)
        partitions = self.particiones_del_disco(disk_device)
        id_fuente, id_raw = self.obtener_huella_disco(device)
        disk_id = self.crear_id_disco(device)
        return {
            "id": disk_id,
            "nombre": self.obtener_nombre(device, disk_device),
            "serial": disk_device.get("ID_SERIAL") or disk_device.get("ID_SERIAL_SHORT"),
            "wwn": disk_device.get("ID_WWN"),
            "tabla_particiones_uuid": self.uuid_tabla_particiones(disk_device, partitions),
            "particiones_uuid": self.uuids_particiones(partitions),
            "id_fuente": id_fuente,
            "id_valor": id_raw,
            "modelo": disk_device.get("ID_MODEL"),
            "vendor": disk_device.get("ID_VENDOR"),
            "bus": disk_device.get("ID_BUS"),
            "device_node": disk_device.device_node,
            "sys_name": disk_device.sys_name,
        }

    def identidades_info(self, info):
        identities = []
        for key in ("wwn", "tabla_particiones_uuid"):
            value = info.get(key)
            if value:
                identities.append(f"{key}:{value}")

        for value in info.get("particiones_uuid") or []:
            identities.append(f"particion_uuid:{value}")

        if info.get("serial"):
            identities.append(f"serial:{info['serial']}")

        id_source = info.get("id_fuente")
        id_value = info.get("id_valor")
        if id_source in {"wwn", "partition_table_uuid", "partition_uuids", "serial"} and id_value:
            identities.append(f"{id_source}:{id_value}")

        return identities

    def buscar_id_existente(self, info):
        identities = set(self.identidades_info(info))
        if not identities:
            return None

        for disk_id, saved_info in self.registry.items():
            if identities.intersection(self.identidades_info(saved_info)):
                return disk_id
        return None

    def registrar_disco(self, info, action):
        with self.registry_lock:
            now = datetime.now(timezone.utc).isoformat()
            canonical_id = info["id"]
            existing_id = self.buscar_id_existente(info)
            target_id = canonical_id

            if existing_id and existing_id != canonical_id:
                existing_info = self.registry.get(existing_id, {})
                old_source = existing_info.get("id_fuente")
                new_source = info.get("id_fuente")
                if not self.huella_es_fuerte(new_source) or self.huella_es_fuerte(old_source):
                    target_id = existing_id
                else:
                    logger.info(
                        f"Migrando registro {existing_id} a {canonical_id} "
                        f"porque ahora hay huella fuerte ({new_source})."
                    )
            elif existing_id:
                target_id = existing_id

            current = {}
            if existing_id and existing_id in self.registry:
                current.update(self.registry[existing_id])
            current.update(self.registry.get(target_id, {}))
            current.update({key: value for key, value in info.items() if value})
            current["id"] = target_id
            current["ultimo_evento"] = action
            current["ultima_vez_visto"] = now
            current.setdefault("primer_registro", now)

            if existing_id and existing_id != target_id:
                self.registry.pop(existing_id, None)
            self.registry[target_id] = current
            self.guardar_registro()
            info["id"] = target_id

    def punto_montaje_actual(self, device_node):
        result = self.ejecutar(["findmnt", "-rn", "-S", device_node, "-o", "TARGET"], timeout=5)
        if result and result.returncode == 0 and result.stdout.strip():
            return self.decodificar_findmnt(result.stdout.strip().splitlines()[0])

        for part in psutil.disk_partitions(all=True):
            if part.device == device_node:
                return part.mountpoint
        return None

    def esperar_montaje(self, device_node, segundos=AUTO_MOUNT_WAIT_SECONDS):
        for elapsed in range(segundos):
            mountpoint = self.punto_montaje_actual(device_node)
            if mountpoint:
                return mountpoint
            if elapsed in {0, 4}:
                self.avisar(
                    f"Esperando montaje automático de {device_node}... "
                    f"{elapsed + 1}/{segundos}s"
                )
            time.sleep(1)
        return None

    def montar_con_udisksctl(self, device_node):
        self.avisar(f"Intentando montar {device_node} con udisksctl...")
        result = self.ejecutar(
            ["udisksctl", "mount", "-b", device_node],
            timeout=UDISKSCTL_TIMEOUT_SECONDS,
        )
        if not result or result.returncode != 0:
            mountpoint = self.punto_montaje_actual(device_node)
            if mountpoint:
                self.avisar(f"{device_node} ya estaba montado en: {mountpoint}", "success")
                return mountpoint

            if result and result.stderr.strip():
                self.avisar(
                    f"udisksctl no pudo montar {device_node}: {result.stderr.strip()}",
                    "warning",
                )
            return None

        mountpoint = self.esperar_montaje(device_node, segundos=3)
        if mountpoint:
            return mountpoint

        match = re.search(r" at (.+?)\.?$", result.stdout.strip())
        if match:
            return match.group(1)
        return None

    def montar_con_mount_auto(self, device, disk_id):
        device_node = device.device_node
        label = device.get("ID_FS_LABEL") or device.get("ID_FS_LABEL_ENC")
        mount_name = self.limpiar_nombre_montaje(label or f"disk_listener_{disk_id}_{Path(device_node).name}")
        mountpoint = self.crear_punto_montaje_unico(mount_name)
        try:
            mountpoint.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            logger.error(f"Sin permisos para crear {mountpoint}")
            return None

        mount_command = ["mount", "-t", "auto"]
        fs_type = (device.get("ID_FS_TYPE") or "").lower()
        if fs_type in {"vfat", "exfat", "ntfs", "ntfs3"}:
            mount_command.extend(["-o", f"uid={os.getuid()},gid={os.getgid()},umask=022"])
        mount_command.extend([device_node, str(mountpoint)])

        self.avisar(f"Intentando montaje manual de {device_node} en {mountpoint}...")
        result = self.ejecutar(mount_command, timeout=MANUAL_MOUNT_TIMEOUT_SECONDS)
        if result and result.returncode == 0:
            return str(mountpoint)

        if result and result.stderr.strip():
            self.avisar(
                f"mount -t auto falló para {device_node}: {result.stderr.strip()}",
                "warning",
            )
        return None

    def montar_particion(self, device, disk_id):
        device_node = device.device_node
        if not device_node:
            return None

        if not self.es_particion_montable(device):
            self.avisar(
                f"{device_node} detectado, pero no tiene filesystem montable.",
                "warning",
            )
            return None

        fs_type = device.get("ID_FS_TYPE") or "desconocido"
        label = device.get("ID_FS_LABEL") or device.get("ID_FS_LABEL_ENC") or "sin etiqueta"
        self.avisar(f"Partición montable detectada: {device_node} | FS: {fs_type} | Nombre: {label}")

        mountpoint = self.esperar_montaje(device_node)
        if mountpoint:
            self.avisar(f"{device_node} ya está montado en: {mountpoint}", "success")
            return mountpoint

        mountpoint = self.montar_con_udisksctl(device_node)
        if mountpoint:
            self.avisar(f"{device_node} montado con udisksctl en: {mountpoint}", "success")
            return mountpoint

        mountpoint = self.montar_con_mount_auto(device, disk_id)
        if mountpoint:
            self.avisar(f"{device_node} montado manualmente en: {mountpoint}", "success")
            return mountpoint

        self.avisar(
            f"No se pudo montar {device_node} después de "
            f"{AUTO_MOUNT_WAIT_SECONDS + UDISKSCTL_TIMEOUT_SECONDS + MANUAL_MOUNT_TIMEOUT_SECONDS}s. "
            "Revisa disk_listener.log para el error de udisksctl/mount.",
            "error",
        )
        return None

    def iniciar_proceso_individual(self, info, mountpoint):
        if not self.consulter_script.exists():
            self.avisar(
                f"No se encontró {self.consulter_script}; no se iniciará el proceso de copia.",
                "error",
            )
            return

        if not self.consulter_db_dsn:
            self.avisar(
                "CONSULTER_DB_DSN no está definido; no se iniciará el proceso de copia.",
                "warning",
            )
            return

        disk_id = info.get("id") or info.get("nombre") or "unknown_disk"
        with self.process_lock:
            proc = self.disk_processes.get(disk_id)
            if proc is not None and proc.poll() is None:
                self.avisar(
                    f"Ya existe un proceso activo para {disk_id}; no se iniciará otro.",
                    "info",
                )
                return
            self.disk_processes.pop(disk_id, None)

        command = [
            str(self.python_path),
            str(self.consulter_script),
            mountpoint,
            "--disk-id",
            disk_id,
            "--output-path",
            str(self.output_root),
            "--db-dsn",
            self.consulter_db_dsn,
            "--skip-schema-init",
        ]

        log_path = self.process_log_dir / f"{disk_id}.log"
        self.avisar(
            f"Iniciando proceso individual para {disk_id} en {mountpoint}. Log: {log_path}",
            "info",
        )

        try:
            log_file = open(log_path, "a", encoding="utf-8")
        except Exception as exc:
            self.avisar(
                f"No se pudo abrir el log de proceso para {disk_id}: {exc}",
                "error",
            )
            return

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        stdout_pipe = subprocess.PIPE

        try:
            proc = subprocess.Popen(
                command,
                stdout=stdout_pipe,
                stderr=log_file,
                env=env,
                start_new_session=True,
                text=True,
                bufsize=1,
            )
        except Exception as exc:
            self.avisar(
                f"No se pudo iniciar el proceso de copia para {disk_id}: {exc}",
                "error",
            )
            log_file.close()
            return

        log_lock = threading.Lock()
        with self.process_lock:
            self.disk_processes[disk_id] = proc

        threading.Thread(
            target=self.read_process_stdout,
            args=(disk_id, log_file, proc.stdout, log_lock),
            daemon=True,
        ).start()

        def monitor_process():
            exit_code = proc.wait()
            with log_lock:
                log_file.write(f"\n=== Proceso {disk_id} finalizó con código {exit_code} ===\n")
                log_file.flush()
                log_file.close()
            with self.process_lock:
                self.disk_processes.pop(disk_id, None)
                remaining = len(self.disk_processes)
            if exit_code == 0:
                self.avisar(
                    f"Proceso individual completado: {disk_id} -> {mountpoint}",
                    "success",
                )
            else:
                self.avisar(
                    f"Proceso individual falló ({disk_id}) con código {exit_code}",
                    "error",
                )
            if remaining == 0:
                self.avisar(
                    "Todos los discos conectados han terminado. Puedes desconectar uno y conectar otro.",
                    "success",
                )

        threading.Thread(target=monitor_process, daemon=True).start()

    def manejar_add(self, device):
        devtype = device.get("DEVTYPE")
        if devtype not in {"disk", "partition"}:
            return

        device = self.refrescar_device(device)
        devtype = device.get("DEVTYPE")

        if not self.es_particion_montable(device):
            if devtype == "disk":
                disk_node = device.device_node or device.sys_name
                self.avisar(f"Disco físico detectado: {disk_node}. Buscando particiones montables...")
                mountable_partitions = self.esperar_particiones_montables(device)
                if not mountable_partitions:
                    self.reintentar_lectura_particiones(device)
                    mountable_partitions = self.esperar_particiones_montables(device, segundos=5)

                if not mountable_partitions:
                    info = self.obtener_info_disco(device)
                    self.registrar_disco(info, "connect_failed")
                    with self.active_lock:
                        self.active_devices[device.sys_name] = info
                        if device.device_node:
                            self.active_devices[device.device_node] = info
                    table_type = device.get("ID_PART_TABLE_TYPE") or "sin tabla"
                    table_uuid = device.get("ID_PART_TABLE_UUID") or "sin uuid"
                    self.avisar(
                        f"Conectado: {info['nombre']} | ID: {info['id']} | "
                        f"Dispositivo: {disk_node} | No se pudo montar: "
                        "el disco fue detectado, pero no existe ninguna partición montable "
                        f"({table_type}, {table_uuid}).",
                        "error",
                    )
                    return

                for partition in mountable_partitions:
                    self.procesar_evento("add", partition)
                return

            self.avisar(
                f"Conectado: {device.device_node or device.sys_name} | "
                "No se pudo montar: sin filesystem detectado",
                "warning",
            )
            return

        id_fuente, _ = self.obtener_huella_disco(device)
        if devtype == "disk" and not self.huella_es_fuerte(id_fuente):
            self.avisar(
                f"Detectado {device.device_node or device.sys_name}, "
                f"pero la huella aun es debil ({id_fuente}); intentando montar de todos modos."
            )

        info = self.obtener_info_disco(device)
        mountpoint = None
        if self.es_particion_montable(device):
            mountpoint = self.montar_particion(device, info["id"])
            info["particion"] = device.device_node
            info["filesystem"] = device.get("ID_FS_TYPE")
            info["filesystem_uuid"] = device.get("ID_FS_UUID")
            info["particion_uuid"] = device.get("ID_PART_ENTRY_UUID")
            info["label"] = device.get("ID_FS_LABEL") or device.get("ID_FS_LABEL_ENC")
            if mountpoint:
                info["mountpoint"] = mountpoint

        self.registrar_disco(info, "connect")
        with self.active_lock:
            self.active_devices[device.sys_name] = info
            if device.device_node:
                self.active_devices[device.device_node] = info

        mensaje = f"Conectado: {info['nombre']} | ID: {info['id']}"
        if device.device_node:
            mensaje += f" | Dispositivo: {device.device_node}"
        if mountpoint:
            mensaje += f" | Montado en: {mountpoint}"
            self.avisar(mensaje, "success")
            self.iniciar_proceso_individual(info, mountpoint)
        else:
            mensaje += " | No se pudo montar"
            self.avisar(mensaje, "warning")

    def obtener_id_seguro(self, raw_value):
        safe = re.sub(r"[^A-Za-z0-9]+", "_", str(raw_value).strip())
        safe = re.sub(r"_+", "_", safe).strip("_")
        return safe.upper() if safe else "DISK"

    def scan_existing_mounts(self):
        if not MOUNT_BASE.exists():
            return

        partitions = psutil.disk_partitions(all=False)
        for part in partitions:
            mountpoint = part.mountpoint
            if not mountpoint.startswith(str(MOUNT_BASE)):
                continue
            if mountpoint == str(MOUNT_BASE):
                continue

            device_node = part.device
            self.avisar(f"Montaje existente detectado: {mountpoint} ({device_node})", "info")
            try:
                device = pyudev.Devices.from_device_file(self.context, device_node)
            except Exception:
                device = None

            if device is not None:
                self.procesar_evento("add", device)
            else:
                disk_id = self.obtener_id_seguro(Path(mountpoint).name)
                info = {"id": disk_id, "nombre": Path(mountpoint).name}
                self.iniciar_proceso_individual(info, mountpoint)

    def test_db_connection(self):
        if not self.consulter_db_dsn:
            self.avisar(
                "CONSULTER_DB_DSN no está definido; no se puede iniciar el listener.",
                "error",
            )
            return False
        try:
            conn = psycopg2.connect(self.consulter_db_dsn)
            conn.close()
            return True
        except Exception as exc:
            self.avisar(
                f"Error de conexión a la base de datos: {exc}",
                "error",
            )
            return False

    def format_progress_bar(self, current: int, total: int, width: int = 30) -> str:
        if total <= 0:
            return f"procesados {current} archivos..."
        ratio = min(max(current / total, 0.0), 1.0)
        filled = int(ratio * width)
        bar = "█" * filled + "-" * (width - filled)
        return f"[{bar}] {int(ratio * 100):3d}% ({current}/{total})"

    def update_progress(self, disk_id: str, current: int, total: int):
        with self.progress_lock:
            self.progress_status[disk_id] = (current, total)
            bar = self.format_progress_bar(current, total)
            print(f"[{disk_id}] {bar}", flush=True)

    def read_process_stdout(self, disk_id: str, log_file, stdout_pipe, log_lock):
        try:
            assert stdout_pipe is not None
            for raw_line in iter(stdout_pipe.readline, ""):
                if not raw_line:
                    break
                stripped = raw_line.rstrip("\n")
                if stripped.startswith("PROGRESS:"):
                    parts = stripped.split(":")
                    if len(parts) == 4:
                        _, parsed_id, current, total = parts
                        if parsed_id == disk_id:
                            try:
                                self.update_progress(parsed_id, int(current), int(total))
                            except ValueError:
                                pass
                    continue
                with log_lock:
                    log_file.write(raw_line)
                    log_file.flush()
        finally:
            if stdout_pipe is not None:
                stdout_pipe.close()

    def manejar_remove(self, device):
        with self.active_lock:
            info = self.active_devices.get(device.sys_name)
            if not info and device.device_node:
                info = self.active_devices.get(device.device_node)
        if not info:
            info = self.obtener_info_disco(device)

        self.registrar_disco(info, "disconnect")
        mensaje = f"Desconectado: {info['nombre']} | ID: {info['id']}"
        if device.device_node:
            mensaje += f" | Dispositivo: {device.device_node}"

        self.avisar(mensaje, "warning")

    def clave_evento(self, action, device):
        return action, device.device_node or device.sys_name or device.device_path

    def procesar_evento(self, action, device):
        key = self.clave_evento(action, device)
        with self.processing_lock:
            if key in self.processing_devices:
                return
            self.processing_devices.add(key)

        try:
            if action == "add":
                self.manejar_add(device)
            elif action == "remove":
                self.manejar_remove(device)
        except Exception as exc:
            logger.exception(f"Error procesando evento {action}: {exc}")
        finally:
            with self.processing_lock:
                self.processing_devices.discard(key)

    def iniciar(self):
        self.avisar("Listener de discos activo. Conecta o desconecta un disco.")
        if not self.test_db_connection():
            self.avisar("El listener no puede iniciarse sin conexión válida a la base de datos.", "error")
            return

        if not self.initialize_db_schema():
            self.avisar("No se pudo inicializar el esquema de la base de datos. El listener no continuará.", "error")
            return

        self.scan_existing_mounts()

        for action, device in self.monitor:
            thread = threading.Thread(
                target=self.procesar_evento,
                args=(action, device),
                daemon=True,
            )
            thread.start()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Listener de discos que lanza un proceso independiente por disco conectado."
    )
    parser.add_argument(
        "--db-dsn",
        default=DEFAULT_CONSULTER_DB_DSN,
        help="Cadena DSN de PostgreSQL para el copiador (puede usarse CONSULTER_DB_DSN).",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_COPY_ROOT),
        help="Raíz donde se guardarán los archivos copiados.",
    )
    parser.add_argument(
        "--python",
        default=str(DEFAULT_PYTHON),
        help="Ruta al intérprete de Python para ejecutar consulter_copier.py.",
    )
    parser.add_argument(
        "--log-path",
        default=str(LOG_PATH),
        help="Ruta del log del listener.",
    )
    parser.add_argument(
        "--process-log-dir",
        default=str(BASE_DIR / "disk_listener_logs"),
        help="Directorio donde se guardarán los logs de cada proceso individual.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    logger.remove()
    logger.add(args.log_path, rotation="10 MB", level="INFO")
    app = DiskListener()
    app.consulter_db_dsn = args.db_dsn
    app.output_root = Path(args.output_root)
    app.python_path = Path(args.python)
    app.process_log_dir = Path(args.process_log_dir)
    app.process_log_dir.mkdir(parents=True, exist_ok=True)
    app.iniciar()
